package org.linkja.hashing;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * This represents a consolidated collection of parameters needed to run the engine.
 */
public class EngineParameters {
  public enum RecordExceptionMode {
    NoExceptions, GenerateExceptions, ExceptionsIncluded
  }

  public static final int MIN_NUM_WORKER_THREADS = 1;
  public static final int MIN_BATCH_SIZE = 100;
  public static final int MIN_SALT_LENGTH = 1;

  public static final char DEFAULT_DELIMITER = ',';
  public static final RecordExceptionMode DEFAULT_RECORD_EXCEPTION_MODE = RecordExceptionMode.NoExceptions;
  public static final int DEFAULT_WORKER_THREADS = 3;
  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final boolean DEFAULT_RUN_NORMALIZATION_STEP = true;
  public static final boolean DEFAULT_WRITE_UNHASHED_DATA = false;
  public static final int DEFAULT_MIN_SALT_LENGTH = 13;

  public static final DateTimeFormatter PrivateDateFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");

  private File privateKeyFile;
  private File saltFile;
  private File patientFile;
  private LocalDate privateDate;
  private Path outputDirectory;
  private char delimiter = DEFAULT_DELIMITER;
  private RecordExceptionMode recordExceptionMode = DEFAULT_RECORD_EXCEPTION_MODE;
  private int numWorkerThreads = DEFAULT_WORKER_THREADS;
  private int batchSize = DEFAULT_BATCH_SIZE;
  private boolean runNormalizationStep = DEFAULT_RUN_NORMALIZATION_STEP;
  private FileHelper fileHelper;
  private boolean writeUnhashedData = DEFAULT_WRITE_UNHASHED_DATA;
  private int minSaltLength = DEFAULT_MIN_SALT_LENGTH;

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

  public LocalDate getPrivateDate() {
    return privateDate;
  }

  public void setPrivateDate(LocalDate privateDate) {
    this.privateDate = privateDate;
  }

  public void setPrivateDate(String privateDate) throws ParseException {
    LocalDate parsedDate = LocalDate.parse(privateDate, PrivateDateFormatter);
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

  public RecordExceptionMode getRecordExceptionMode() {
    return recordExceptionMode;
  }

  public void setRecordExceptionMode(RecordExceptionMode recordExceptionmode) {
    this.recordExceptionMode = recordExceptionmode;
  }

  public int getNumWorkerThreads() {
    return numWorkerThreads;
  }

  public void setNumWorkerThreads(int numWorkerThreads) {
    if (numWorkerThreads < MIN_NUM_WORKER_THREADS) {
      throw new InvalidParameterException(String.format("The number of worker threads must be >=%d", MIN_NUM_WORKER_THREADS));
    }
    this.numWorkerThreads = numWorkerThreads;
  }

  public void setNumWorkerThreads(String numWorkerThreads) {
    setNumWorkerThreads((numWorkerThreads == null) ? -1 : Integer.parseInt(numWorkerThreads.trim()));
  }

  public boolean isRunNormalizationStep() {
    return runNormalizationStep;
  }

  public void setRunNormalizationStep(boolean runNormalizationStep) {
    this.runNormalizationStep = runNormalizationStep;
  }

  public void setRunNormalizationStep(String runNormalization) {
    this.runNormalizationStep = (runNormalization == null) ?
            EngineParameters.DEFAULT_RUN_NORMALIZATION_STEP : Boolean.parseBoolean(runNormalization.trim());
  }

  public boolean isWriteUnhashedData() {
    return writeUnhashedData;
  }

  public void setWriteUnhashedData(boolean writeUnhashedData) {
    this.writeUnhashedData = writeUnhashedData;
  }

  public void setWriteUnhashedData(String writeUnhashedData) {
    this.writeUnhashedData = (writeUnhashedData == null) ?
            EngineParameters.DEFAULT_WRITE_UNHASHED_DATA : Boolean.parseBoolean(writeUnhashedData.trim());
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    if (batchSize < MIN_BATCH_SIZE) {
      throw new InvalidParameterException(String.format("The batch size must be >=%d", MIN_BATCH_SIZE));
    }
    this.batchSize = batchSize;
  }

  public void setBatchSize(String batchSize) {
    setBatchSize((batchSize == null) ? -1 : Integer.parseInt(batchSize.trim()));
  }

  public int getMinSaltLength() {
    return minSaltLength;
  }

  public void setMinSaltLength(int minSaltLength) {
    if (minSaltLength < MIN_SALT_LENGTH) {
      throw new InvalidParameterException(String.format("The minimum allowable salt length must be >=%d", MIN_SALT_LENGTH));
    }
    this.minSaltLength = minSaltLength;
  }

  public void setMinSaltLength(String minSaltLength) {
    setMinSaltLength((minSaltLength == null) ? -1 : Integer.parseInt(minSaltLength.trim()));
  }
}
