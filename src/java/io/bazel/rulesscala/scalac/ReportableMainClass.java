package io.bazel.rulesscala.scalac;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import scala.tools.nsc.Global;
import scala.tools.nsc.MainClass;
import scala.tools.nsc.Settings;
import scala.tools.nsc.reporters.ConsoleReporter;
import scala.tools.nsc.reporters.Reporter;

public class ReportableMainClass extends MainClass {
  private Global compiler;
  private Reporter reporter;
  private final CompileOptions ops;
  private PrintStream out;
  private PrintStream err;

  public ReportableMainClass(CompileOptions ops, PrintStream out, PrintStream err) {
    this.ops = ops;
    this.out = out;
    this.err = err;
  }

  @Override
  public Global newCompiler() {
    if (!ops.enableDiagnosticsReport) {
      out.println("diagnostics");
      createDiagnosticsFile();
      Global global = super.newCompiler();
      reporter = new ConsoleReporter(super.settings(), null, new PrintWriter(out), new PrintWriter(err));
      //out.println(reporter);
      return global;
    }

    if (compiler == null) {
      out.println("compiler null");
      createDiagnosticsFile();

      Settings settings = super.settings();
      reporter = new ProtoReporter(settings, out, err);

      compiler = new Global(settings, reporter);
    }
    return compiler;
  }

  private void createDiagnosticsFile() {
    Path path = Paths.get(ops.diagnosticsFile);
    try {
      Files.deleteIfExists(path);
      Files.createFile(path);
    } catch (IOException e) {
      throw new RuntimeException("Could not delete/make diagnostics proto file", e);
    }
  }

  public Reporter getReporter() {
    return this.reporter;
  }
}
