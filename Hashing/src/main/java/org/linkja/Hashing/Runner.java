package org.linkja.Hashing;

import org.apache.commons.cli.*;

import java.nio.file.FileSystems;
import java.nio.file.Path;

public class Runner {
  public static void main(String[] args) {
    Options options = setUpCommandLine();
    CommandLine cmd = parseCommandLine(options, args);

    // If the command line initialization failed, we will stop the program now.  Displaying instructions to the user
    // is already handled by the setup and parser methods.
    if (cmd == null) {
      System.exit(1);
    }

    try {
      EngineParameters parameters = new EngineParameters();
      parameters.setPrivateKeyFile(cmd.getOptionValue("privateKey"));
      parameters.setSaltFile(cmd.getOptionValue("saltFile"));
      parameters.setPatientFile(cmd.getOptionValue("patientFile"));
      parameters.setPrivateDate(cmd.getOptionValue("privateDate"));
//      String outputDirectory = cmd.getOptionValue("outDirectory");
//      if (outputDirectory == null || outputDirectory.equals("")) {
//        Path path = FileSystems.getDefault().getPath(".").toAbsolutePath();
//        outputDirectory = path.toString();
//      }
//      parameters.setOutputDirectory(outputDirectory);

      Engine engine = new Engine(parameters);
      engine.run();
    }
    catch (Exception exc) {
      displayCommandLineException(exc, options);
      System.exit(1);
    }
  }

  private static Options setUpCommandLine() {
    Options options = new Options();

    Option keyFileOpt = new Option("key", "privateKey", true, "path to private key file");
    keyFileOpt.setRequired(true);
    options.addOption(keyFileOpt);

    Option saltFileOpt = new Option("salt", "saltFile", true, "path to encrypted salt file");
    saltFileOpt.setRequired(true);
    options.addOption(saltFileOpt);

    Option patientFileOpt = new Option("patient", "patientFile", true, "path to the file containing patient data");
    patientFileOpt.setRequired(true);
    options.addOption(patientFileOpt);

    Option privateDateOpt = new Option("date", "privateDate", true, "the private date (as MM/DD/YYYY)");
    privateDateOpt.setRequired(true);
    options.addOption(privateDateOpt);

    Option outputDirectoryOpt = new Option("out", "outDirectory", true, "the base directory to create output.  If not specified, will use the current directory.");
    outputDirectoryOpt.setRequired(false);
    options.addOption(outputDirectoryOpt);

    return options;
  }

  /**
   * Helper method to wrap parsing the command line parameters and reconcile them against the required and optional
   * command line options.
   * @param options Allowed options
   * @param args Actual command line arguments
   * @return Reconciled CommandLine container, or null if unable to process
   */
  private static CommandLine parseCommandLine(Options options, String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      displayCommandLineException(e, options);
      return null;
    }

    return cmd;
  }

  /**
   * Helper method to display expected command line parameters, in response to some exception thrown
   * @param exc Exception thrown to display
   * @param options Expected command line options
   */
  private static void displayCommandLineException(Exception exc, Options options) {
    System.out.println(exc.getMessage());
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Hashing", options);
  }
}
