package org.linkja.hashing;


import org.linkja.core.*;
import org.apache.commons.cli.*;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.*;
import java.util.Properties;

// TODO: Write unit tests for helper methods (everything but main)
public class Runner {
  private static String SALT_FILE_DELIMITER = ",";
  private static int NUM_SALT_PARTS = 5;

  public static void main(String[] args) {
    Options options = setUpCommandLine();
    CommandLine cmd = parseCommandLine(options, args);

    // If the command line initialization failed, we will stop the program now.  Displaying instructions to the user
    // is already handled by the setup and parser methods.
    if (cmd == null) {
      System.exit(1);
    }

    long startTime = System.nanoTime();

    // Perform some initialization - purposefully done after timing has started.
    Security.addProvider(new BouncyCastleProvider());

    // There are two modes under which our program can run - hashing and displaySalt.  Depending on which of these
    // modes is set, we have different requirements for other parameters.
    //
    // Our parameters come from the command line and from a local config.properties file.  We split these up under the
    // assumption that those most likely to change across run should be passed in the command line, and those most
    // likely to remain consistent across runs should be in a config.properties file.
    EngineParameters parameters = new EngineParameters();
    try {
      // First get which mode is set
      parameters.setHashingMode(cmd.hasOption("hashing"));
      parameters.setDisplaySaltMode(cmd.hasOption("displaySalt"));

      // Get the additional parameters that may be set
      parameters.setPrivateKeyFile(cmd.getOptionValue("privateKey"));
      parameters.setSaltFile(cmd.getOptionValue("saltFile"));
      parameters.setPatientFile(cmd.getOptionValue("patientFile"));
      parameters.setPrivateDate(cmd.getOptionValue("privateDate"));
      parameters.setEncryptionKeyFile(cmd.getOptionValue("encryptionKey"));
      parameters.setDelimiter(cmd.getOptionValue("delimiter", new String(new char[] { EngineParameters.DEFAULT_DELIMITER })));
      parameters.setRecordExclusionMode(parseRecordExclusionMode(cmd.getOptionValue("exclusionMode")));
      parameters.setWriteUnhashedData(cmd.hasOption("writeUnhashed"));
      parameters = loadConfig(parameters);
      String outputDirectory = cmd.getOptionValue("outDirectory");
      if (outputDirectory == null || outputDirectory.equals("")) {
        Path path = FileSystems.getDefault().getPath("").toAbsolutePath();
        outputDirectory = path.toString();
        if (parameters.isHashingMode()) {
          System.out.printf("Results will be written to %s\r\n", outputDirectory);
        }
      }
      parameters.setOutputDirectory(outputDirectory);

      if (parameters.isDisplaySaltMode() && !parameters.displaySaltModeOptionsSet()) {
        throw new LinkjaException("Not all of the required parameters were provided");
      }
      else if (parameters.isHashingMode() && !parameters.hashingModeOptionsSet()) {
        throw new LinkjaException("Not all of the required parameters were provided");
      }
      else if (!parameters.isDisplaySaltMode() && !parameters.isHashingMode()) {
        throw new LinkjaException("Please specify either --hashing or --displaySalt");
      }
    }
    catch (Exception exc) {
      displayUsage();
      System.out.println();
      System.out.println(exc.getMessage());
      System.exit(1);
    }

    HashParameters hashParameters = null;
    try {
      hashParameters = parseProjectSalt(parameters.getSaltFile(), parameters.getPrivateKeyFile(), parameters.getMinSaltLength());
      hashParameters.setPrivateDate(parameters.getPrivateDate());  // Provide a copy to our hashing parameters collection
    }
    catch (Exception exc) {
      System.out.println("ERROR - We encountered an error when trying to decrypt the salt file using the private key");
      System.out.println(exc.getMessage());
      System.exit(1);
    }

    try {
      // Displaying the salt file may or may not be set
      if (parameters.isDisplaySaltMode()) {
        System.out.println("Salt File Contents");
        System.out.printf("  Site ID:      %s\n", hashParameters.getSiteId());
        System.out.printf("  Site Name:    %s\n", hashParameters.getSiteName());
        System.out.printf("  Private Salt: %s\n", hashParameters.getPrivateSalt());
        System.out.printf("  Project Salt: %s\n", hashParameters.getProjectSalt());
        System.out.printf("  Project ID:   %s\n", hashParameters.getProjectId());
      }

      // Hashing mode may or may not be set
      if (parameters.isHashingMode()) {
        Engine engine = new Engine(parameters, hashParameters);
        engine.run();

        String report = String.join("\r\n", engine.getExecutionReport());
        System.out.println(report);
      }
    }
    catch (Exception exc) {
      System.out.println("ERROR - We encountered an error while processing your data file");
      System.out.println(exc.getMessage());
      System.exit(1);
    }

    long endTime = System.nanoTime();

    double elapsedSeconds = (double)(endTime - startTime) / 1_000_000_000.0;
    System.out.printf("Total execution time: %2f sec\n", elapsedSeconds);
  }

  public static HashParameters parseProjectSalt(File saltFile, File privateKeyFile, int minSaltLength) throws Exception {
    CryptoHelper helper = new CryptoHelper();
    String decryptedMessage = new String(helper.decryptRSA(saltFile, privateKeyFile), StandardCharsets.UTF_8);
    String[] saltParts = decryptedMessage.split(SALT_FILE_DELIMITER);
    if (saltParts == null || saltParts.length < NUM_SALT_PARTS) {
      throw new LinkjaException("The salt file was not in the expected format.  Please confirm that you are referencing the correct file");
    }

    // At this point we have to assume that everything is in the right position, so we will load by position.
    HashParameters parameters = new HashParameters();
    parameters.setSiteId(saltParts[0]);
    parameters.setSiteName(saltParts[1]);
    parameters.setPrivateSalt(saltParts[2]);
    parameters.setProjectSalt(saltParts[3]);
    parameters.setProjectId(saltParts[4]);

    if (parameters.getProjectSalt().length() < minSaltLength) {
      throw new LinkjaException(String.format("The project salt must be at least %d characters long, but the one provided is %d",
              minSaltLength, parameters.getProjectSalt().length()));
    }
    if (parameters.getPrivateSalt().length() < minSaltLength) {
      throw new LinkjaException(String.format("The private (site-specific) salt must be at least %d characters long, but the one provided is %d",
              minSaltLength, parameters.getPrivateSalt().length()));
    }

    return parameters;
  }

  /**
   * Load the configuration properties file for the hashing system, and add the values into the EngineParameters
   * @param parameters The collection of parameters used by the engine
   * @return A modified collection of parameters used by the engine
   * @throws IOException
   */
  public static EngineParameters loadConfig(EngineParameters parameters) throws IOException, LinkjaException {
    InputStream inputStream = null;
    try {
      Properties prop = new Properties();
      inputStream = Runner.class.getClassLoader().getResourceAsStream("config.properties");

      if (inputStream != null) {
        prop.load(inputStream);
      }
      else {
        throw new FileNotFoundException("The properties file 'config.properties' could not be found in the classpath");
      }

      parameters.setRecordExclusionMode(parseRecordExclusionMode(prop.getProperty("recordExclusionMode")));
      parameters.setRunNormalizationStep(prop.getProperty("runNormalizationStep", Boolean.toString(EngineParameters.DEFAULT_RUN_NORMALIZATION_STEP)));
      parameters.setNumWorkerThreads(prop.getProperty("workerThreads", Integer.toString(EngineParameters.DEFAULT_WORKER_THREADS)));
      parameters.setBatchSize(prop.getProperty("batchSize", Integer.toString(EngineParameters.DEFAULT_BATCH_SIZE)));
      parameters.setMinSaltLength(prop.getProperty("minSaltLength", Integer.toString(EngineParameters.DEFAULT_MIN_SALT_LENGTH)));
    }
    finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }

    return parameters;
  }

  /**
   * Utility method to take a string representation of the record exception mode, and convert it to an internal enum
   * @param option The string value to process
   * @return The EngineParameters.RecordExclusionMode enum value
   * @throws LinkjaException
   */
  public static EngineParameters.RecordExclusionMode parseRecordExclusionMode(String option) throws LinkjaException {
    if (option == null || option.isEmpty()) {
      return EngineParameters.DEFAULT_RECORD_EXCLUSION_MODE;
    }

    if (option.equalsIgnoreCase("None")) {
      return EngineParameters.RecordExclusionMode.NoExclusions;
    }
    else if (option.equalsIgnoreCase("Generate")) {
      return EngineParameters.RecordExclusionMode.GenerateExclusions;
    }
    else if (option.equalsIgnoreCase("Included")) {
      return EngineParameters.RecordExclusionMode.ExclusionsIncluded;
    }

    throw new LinkjaException("For the exclusionMode parameter, please specify a value of None, Generate or Included");
  }

  /**
   * Helper method to prepare our command line options
   * @return Collection of options to be used at the command line
   */
  public static Options setUpCommandLine() {
    Options options = new Options();

    Option displaySaltOpt = new Option("ds", "displaySalt", false, "display the salt file contents");
    displaySaltOpt.setRequired(false);
    options.addOption(displaySaltOpt);

    Option hashingOpt = new Option("h", "hashing", false, "perform hashing on an input file");
    hashingOpt.setRequired(false);
    options.addOption(hashingOpt);

    Option keyFileOpt = new Option("key", "privateKey", true, "path to private key file");
    keyFileOpt.setRequired(true);
    options.addOption(keyFileOpt);

    Option encryptionKeyFileOpt = new Option("enc", "encryptionKey", true, "path to asymmetric encryption key file to encrypt hashed output");
    encryptionKeyFileOpt.setRequired(false);
    options.addOption(encryptionKeyFileOpt);

    Option saltFileOpt = new Option("salt", "saltFile", true, "path to encrypted salt file");
    saltFileOpt.setRequired(true);
    options.addOption(saltFileOpt);

    Option patientFileOpt = new Option("patient", "patientFile", true, "path to the file containing patient data");
    patientFileOpt.setRequired(false);
    options.addOption(patientFileOpt);

    Option privateDateOpt = new Option("date", "privateDate", true, "the private date (as MM/DD/YYYY)");
    privateDateOpt.setRequired(false);
    options.addOption(privateDateOpt);

    Option outputDirectoryOpt = new Option("out", "outDirectory", true, "the base directory to create output.  If not specified, will use the current directory.");
    outputDirectoryOpt.setRequired(false);
    options.addOption(outputDirectoryOpt);

    Option delimiterOpt = new Option("delim", "delimiter", true, "the delimiter used within the patient data file");
    delimiterOpt.setRequired(false);
    options.addOption(delimiterOpt);

    Option writeUnhashedOpt = new Option("unhashed", "writeUnhashed", false, "write out the original unhashed data in the result file (for debugging");
    writeUnhashedOpt.setRequired(false);
    options.addOption(writeUnhashedOpt);

    return options;
  }

  /**
   * Helper method to wrap parsing the command line parameters and reconcile them against the required and optional
   * command line options.
   * @param options Allowed options
   * @param args Actual command line arguments
   * @return Reconciled CommandLine container, or null if unable to process
   */
  public static CommandLine parseCommandLine(Options options, String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      displayUsage();
      System.out.println();
      System.out.println(e.getMessage());
      return null;
    }

    return cmd;
  }

  /**
   * Helper method to display expected command line parameters
   */
  public static void displayUsage() {
    System.out.println();
    System.out.println("Usage: java -jar Hashing.jar [--hashing | --displaySalt]");
    System.out.println();
    System.out.println("HASHING");
    System.out.println("-------------");
    System.out.println("Required parameters:");
    System.out.println("  -h,--hashing                      Perform hashing");
    System.out.println("  -date,--privateDate <arg>         The private date (as MM/DD/YYYY)");
    System.out.println("  -key,--privateKey <arg>           Path to the private key file");
    System.out.println("  -patient,--patientFile <arg>      Path to the file containing patient data");
    System.out.println("  -salt,--saltFile <arg>            Path to encrypted salt file");
    System.out.println();
    System.out.println("Optional parameters:");
    System.out.println("  -out,--outDirectory <arg>         The base directory to create output. If not");
    System.out.println("                                    specified, will use the current directory.");
    System.out.println("  -delim,--delimiter <arg>          The delimiter used within the patient data");
    System.out.println("                                    file. Uses a comma \",\" by default.");
    System.out.println("  -unhashed,--writeUnhashed         write out the original unhashed data in the ");
    System.out.println("                                    result file (for debugging). false by default.");
    System.out.println();
    System.out.println("DISPLAY SALT");
    System.out.println("-------------");
    System.out.println("Required parameters:");
    System.out.println("  -ds,--displaySalt                 Display the contents of the salt file");
    System.out.println("  -key,--privateKey <arg>           Path to the private key file");
    System.out.println("  -salt,--saltFile <arg>            Path to encrypted salt file");
    System.out.println();
  }
}
