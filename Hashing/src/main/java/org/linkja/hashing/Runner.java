package org.linkja.hashing;

import org.apache.commons.cli.*;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
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

    // Our parameters come from the command line and from a local config.properties file.  We split these up under the
    // assumption that those most likely to change across run should be passed in the command line, and those most
    // likely to remain consistent across runs should be in a config.properties file.
    EngineParameters parameters = new EngineParameters();
    try {
      parameters.setPrivateKeyFile(cmd.getOptionValue("privateKey"));
      parameters.setSaltFile(cmd.getOptionValue("saltFile"));
      parameters.setPatientFile(cmd.getOptionValue("patientFile"));
      parameters.setPrivateDate(cmd.getOptionValue("privateDate"));
      parameters.setDelimiter(cmd.getOptionValue("delimiter", new String(new char[] { EngineParameters.DEFAULT_DELIMITER })));
      parameters.setRecordExceptionMode(parseRecordExceptionMode(cmd.getOptionValue("exceptionMode")));
      parameters = loadConfig(parameters);
//      String outputDirectory = cmd.getOptionValue("outDirectory");
//      if (outputDirectory == null || outputDirectory.equals("")) {
//        Path path = FileSystems.getDefault().getPath(".").toAbsolutePath();
//        outputDirectory = path.toString();
//      }
//      parameters.setOutputDirectory(outputDirectory);
    }
    catch (Exception exc) {
      displayCommandLineException(exc, options);
      System.exit(1);
    }

    HashParameters hashParameters = null;
    try {
      hashParameters = parseProjectSalt(parameters.getSaltFile(), parameters.getPrivateKeyFile());
      hashParameters.setPrivateDate(parameters.getPrivateDate());  // Provide a copy to our hashing parameters collection
    }
    catch (Exception exc) {
      System.out.println("ERROR - We encountered an error when trying to decrypt the salt file using the private key");
      System.out.println(exc.getMessage());
      System.exit(1);
    }

    try {
      Engine engine = new Engine(parameters, hashParameters);
      engine.run();
    }
    catch (Exception exc) {
      System.out.println("ERROR - We encountered an error while processing your data file");
      System.out.println(exc.getMessage());
      System.exit(1);
    }
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

      parameters.setRecordExceptionMode(parseRecordExceptionMode(prop.getProperty("recordExceptionMode")));
      parameters.setRunNormalizationStep(prop.getProperty("runNormalizationStep", Boolean.toString(EngineParameters.DEFAULT_RUN_NORMALIZATION_STEP)));
      parameters.setNumWorkerThreads(prop.getProperty("workerThreads", Integer.toString(EngineParameters.DEFAULT_WORKER_THREADS)));
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
   * @return The EngineParameters.RecordExceptionMode enum value
   * @throws LinkjaException
   */
  public static EngineParameters.RecordExceptionMode parseRecordExceptionMode(String option) throws LinkjaException {
    if (option == null || option.isEmpty()) {
      return EngineParameters.DEFAULT_RECORD_EXCEPTION_MODE;
    }

    if (option.equalsIgnoreCase("None")) {
      return EngineParameters.RecordExceptionMode.NoExceptions;
    }
    else if (option.equalsIgnoreCase("Generate")) {
      return EngineParameters.RecordExceptionMode.GenerateExceptions;
    }
    else if (option.equalsIgnoreCase("Included")) {
      return EngineParameters.RecordExceptionMode.ExceptionsIncluded;
    }

    throw new LinkjaException("For the exceptionMode parameter, please specify a value of None, Generate or Included");
  }

  /**
   * Helper method to prepare our command line options
   * @return Collection of options to be used at the command line
   */
  public static Options setUpCommandLine() {
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

    Option delimiterOpt = new Option("delim", "delimiter", true, "the delimiter used within the patient data file");
    delimiterOpt.setRequired(false);
    options.addOption(delimiterOpt);

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
  public static void displayCommandLineException(Exception exc, Options options) {
    System.out.println(exc.getMessage());
    System.out.println();
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Hashing", options);
  }

  /**
   * Given an encrypted salt file, and a private decryption key, get out the hashing parameters that include site and
   * project details.  Note that the HashParameters are considered sensitive information, because they are encrypted.
   * @param saltFile
   * @param decryptKey
   * @return
   * @throws Exception
   */
  public static HashParameters parseProjectSalt(File saltFile, File decryptKey) throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    BufferedReader reader = new BufferedReader(new FileReader(decryptKey));
    PEMParser parser = new PEMParser(reader);
    PEMKeyPair pemKeyPair = (PEMKeyPair) parser.readObject();
    KeyPair keyPair = new JcaPEMKeyConverter().getKeyPair(pemKeyPair);
    parser.close();
    reader.close();

    Cipher decrypt = Cipher.getInstance("RSA/ECB/PKCS1Padding");
    decrypt.init(Cipher.DECRYPT_MODE, keyPair.getPrivate());
    String decryptedMessage = new String(decrypt.doFinal(Files.readAllBytes(saltFile.toPath())), StandardCharsets.UTF_8);
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

    return parameters;
  }
}
