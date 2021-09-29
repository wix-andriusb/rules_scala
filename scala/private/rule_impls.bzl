# Copyright 2015 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Rules for supporting the Scala language."""

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@bazel_tools//tools/jdk:toolchain_utils.bzl", "find_java_runtime_toolchain", "find_java_toolchain")
load(":common.bzl", _collect_plugin_paths = "collect_plugin_paths")
load(":resources.bzl", _resource_paths = "paths")

def expand_location(ctx, flags):
    if hasattr(ctx.attr, "data"):
        data = ctx.attr.data
    else:
        data = []
    return [ctx.expand_location(f, data) for f in flags]

# Return the first non-empty arg. If all are empty, return the last.
def first_non_empty(*args):
    for arg in args:
        if arg:
            return arg
    return args[-1]

def compile_scala(
        ctx,
        target_label,
        output,
        manifest,
        statsfile,
        diagnosticsfile,
        sources,
        cjars,
        all_srcjars,
        transitive_compile_jars,
        plugins,
        resource_strip_prefix,
        resources,
        resource_jars,
        labels,
        in_scalacopts,
        print_compile_time,
        expect_java_output,
        scalac_jvm_flags,
        scalac,
        dependency_info,
        unused_dependency_checker_ignored_targets,
        stamp_target_label = None):
    # look for any plugins:
    input_plugins = plugins
    plugins = _collect_plugin_paths(plugins)
    if dependency_info.use_analyzer:
        plugins = depset(transitive = [plugins, ctx.attr._dependency_analyzer_plugin.files])

    toolchain = ctx.toolchains["@io_bazel_rules_scala//scala:toolchain_type"]
    compiler_classpath_jars = cjars if dependency_info.dependency_mode == "direct" else transitive_compile_jars
    classpath_resources = getattr(ctx.files, "classpath_resources", [])
    scalacopts = [ctx.expand_location(v, input_plugins) for v in toolchain.scalacopts + in_scalacopts]
    resource_paths = _resource_paths(resources, resource_strip_prefix)
    enable_diagnostics_report = toolchain.enable_diagnostics_report

    args = ctx.actions.args()
    args.set_param_file_format("multiline")
    args.use_param_file(param_file_arg = "@%s", use_always = True)
    args.add("--CurrentTarget", target_label)
    args.add("--StampLabel", stamp_target_label if stamp_target_label != None else target_label)
    args.add("--JarOutput", output)
    args.add("--Manifest", manifest)
    args.add("--PrintCompileTime", print_compile_time)
    args.add("--ExpectJavaOutput", expect_java_output)
    args.add("--StrictDepsMode", dependency_info.strict_deps_mode)
    args.add("--UnusedDependencyCheckerMode", dependency_info.unused_deps_mode)
    args.add("--DependencyTrackingMethod", dependency_info.dependency_tracking_method)
    args.add("--StatsfileOutput", statsfile)
    args.add("--EnableDiagnosticsReport", enable_diagnostics_report)
    args.add("--DiagnosticsFile", diagnosticsfile)
    args.add_all("--Classpath", compiler_classpath_jars)
    args.add_all("--ClasspathResourceSrcs", classpath_resources)
    args.add_all("--Files", sources)
    args.add_all("--Plugins", plugins)
    args.add_all("--ResourceTargets", [p[0] for p in resource_paths])
    args.add_all("--ResourceSources", [p[1] for p in resource_paths])
    args.add_all("--ResourceJars", resource_jars)
    args.add_all("--ScalacOpts", scalacopts)
    args.add_all("--SourceJars", all_srcjars)

    if dependency_info.need_direct_info:
        if dependency_info.need_direct_jars:
            args.add_all("--DirectJars", cjars)
        if dependency_info.need_direct_targets:
            args.add_all("--DirectTargets", [labels[j.path] for j in cjars.to_list()])

    if dependency_info.need_indirect_info:
        args.add_all("--IndirectJars", transitive_compile_jars)
        args.add_all("--IndirectTargets", [labels[j.path] for j in transitive_compile_jars.to_list()])

    if dependency_info.unused_deps_mode != "off":
        args.add_all("--UnusedDepsIgnoredTargets", unused_dependency_checker_ignored_targets)

    outs = [output, statsfile, diagnosticsfile]

    ins = depset(
        direct = [manifest] + sources + classpath_resources + resources + resource_jars,
        transitive = [compiler_classpath_jars, all_srcjars, plugins],
    )

    # scalac_jvm_flags passed in on the target override scalac_jvm_flags passed in on the toolchain
    final_scalac_jvm_flags = first_non_empty(scalac_jvm_flags, toolchain.scalac_jvm_flags)

    ctx.actions.run(
        inputs = ins,
        outputs = outs,
        executable = scalac,
        mnemonic = "Scalac",
        progress_message = "scala %s" % target_label,
        execution_requirements = {"supports-workers": "1", "supports-multiplex-workers": "1"},
        #  when we run with a worker, the `@argfile.path` is removed and passed
        #  line by line as arguments in the protobuf. In that case,
        #  the rest of the arguments are passed to the process that
        #  starts up and stays resident.

        # In either case (worker or not), they will be jvm flags which will
        # be correctly handled since the executable is a jvm app that will
        # consume the flags on startup.
        arguments = [
            "--jvm_flag=%s" % f
            for f in expand_location(ctx, final_scalac_jvm_flags)
        ] + [args],
    )

def compile_java(ctx, source_jars, source_files, output, extra_javac_opts, providers_of_dependencies):
    return java_common.compile(
        ctx,
        source_jars = source_jars,
        source_files = source_files,
        output = output,
        javac_opts = expand_location(
            ctx,
            extra_javac_opts +
            java_common.default_javac_opts(
                java_toolchain = ctx.attr._java_toolchain[java_common.JavaToolchainInfo],
            ),
        ),
        deps = providers_of_dependencies,
        #exports can be empty since the manually created provider exposes exports
        #needs to be empty since we want the provider.compile_jars to only contain the sources ijar
        #workaround until https://github.com/bazelbuild/bazel/issues/3528 is resolved
        exports = [],
        neverlink = getattr(ctx.attr, "neverlink", False),
        java_toolchain = find_java_toolchain(ctx, ctx.attr._java_toolchain),
        host_javabase = find_java_runtime_toolchain(ctx, ctx.attr._host_javabase),
        strict_deps = ctx.fragments.java.strict_java_deps,
    )

def runfiles_root(ctx):
    return "${TEST_SRCDIR}/%s" % ctx.workspace_name

def java_bin(ctx):
    java_path = str(ctx.attr._java_runtime[java_common.JavaRuntimeInfo].java_executable_runfiles_path)
    if paths.is_absolute(java_path):
        javabin = java_path
    else:
        runfiles_root_var = runfiles_root(ctx)
        javabin = "%s/%s" % (runfiles_root_var, java_path)
    return javabin

def is_windows(ctx):
    return ctx.configuration.host_path_separator == ";"
