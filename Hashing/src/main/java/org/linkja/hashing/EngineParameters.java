package org.linkja.hashing;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EngineParameters {
  public static final char DEFAULT_DELIMITER = ',';

  private static final SimpleDateFormat PrivateDateFormatter = new SimpleDateFormat("MM/dd/yyyy");

  private File privateKeyFile;
  private File saltFile;
  private File patientFile;
  private Date privateDate;
  private Path outputDirectory;
  private char delimiter = DEFAULT_DELIMITER;
  private FileHelper fileHelper;

  public EngineParameters() {
    fileHelper = new FileHelper();
  }

  /**
   * Used for unit testing - use empty constructor otherwise
   * @param helper
   */
  public EngineParameters(FileHelper helper) {
    fileHelper = helper;
  }

  public File getPrivateKeyFile() {
    return privateKeyFile;
  }

  public void setPrivateKeyFile(File privateKeyFile) throws FileNotFoundException {
    if (!fileHelper.exists(privateKeyFile)) {
      throw new FileNotFoundException(String.format("Unable to find private key file %s", privateKeyFile.toString()));
    }
    this.privateKeyFile = privateKeyFile;
  }

  public void setPrivateKeyFile(String privateKeyFile) throws FileNotFoundException {
    File file = new File(privateKeyFile);
    setPrivateKeyFile(file);
  }

  public File getSaltFile() {
    return saltFile;
  }

  public void setSaltFile(File saltFile) throws FileNotFoundException {
    if (!fileHelper.exists(saltFile)) {
      throw new FileNotFoundException(String.format("Unable to find encrypted salt file %s", saltFile.toString()));
    }
    this.saltFile = saltFile;
  }

  public void setSaltFile(String saltFile) throws FileNotFoundException {
    File file = new File(saltFile);
    setSaltFile(file);
  }

  public File getPatientFile() {
    return patientFile;
  }

  public void setPatientFile(File patientFile) throws FileNotFoundException {
    if (!fileHelper.exists(patientFile)) {
      throw new FileNotFoundException(String.format("Unable to find patient data file %s", patientFile.toString()));
    }
    this.patientFile = patientFile;
  }

  public void setPatientFile(String patientFile) throws FileNotFoundException {
    File file = new File(patientFile);
    setPatientFile(file);
  }

  public Date getPrivateDate() {
    return privateDate;
  }

  public void setPrivateDate(Date privateDate) {
    this.privateDate = privateDate;
  }

  public void setPrivateDate(String privateDate) throws ParseException {
    Date parsedDate = PrivateDateFormatter.parse(privateDate);
    setPrivateDate(parsedDate);
  }

  public Path getOutputDirectory() {
    return outputDirectory;
  }

  public void setOutputDirectory(Path outputDirectory) throws FileNotFoundException {
    if (!fileHelper.exists(outputDirectory)) {
      throw new FileNotFoundException(String.format("Unable to find output directory %s", outputDirectory.toString()));
    }
    this.outputDirectory = outputDirectory;
  }

  public void setOutputDirectory(String outputDirectory) throws FileNotFoundException {
    Path path = fileHelper.pathFromString(outputDirectory);
    setOutputDirectory(path);
  }

  public char getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(char delimiter) throws LinkjaException {
    String delimiterString = new String(new char[] { delimiter });
    setDelimiter(delimiterString);
  }

  public void setDelimiter(String delimiter) throws LinkjaException {
    if (delimiter == null || delimiter.trim().length() != 1) {
      throw new LinkjaException("You may specify one non-whitespace character as your delimiter");
    }
    this.delimiter = delimiter.trim().charAt(0);
  }
}
