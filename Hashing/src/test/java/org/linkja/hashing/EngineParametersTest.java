package org.linkja.hashing;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;

import static org.junit.jupiter.api.Assertions.*;

class EngineParametersTest {

  @Test
  void setPrivateKeyFile_Found() throws FileNotFoundException {
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(File.class))).thenAnswer(invoke -> true);

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    File file = new File("/test/path/assumed/valid");
    parameters.setPrivateKeyFile(file);
    assertEquals(file.getPath(), parameters.getPrivateKeyFile().getPath());

    // Test with string parameter
    String filePath = file.getPath();
    parameters.setPrivateKeyFile(filePath);
    assertEquals(filePath, parameters.getPrivateKeyFile().getPath());
  }

  @Test
  void setPrivateKeyFile_NotFound() {
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(File.class))).thenAnswer(invoke -> false);

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    File file = new File("/test/path/assumed/invalid");
    assertThrows(FileNotFoundException.class, () -> parameters.setPrivateKeyFile(file));

    String filePath = "/test/path/assumed/invalid";
    assertThrows(FileNotFoundException.class, () -> parameters.setPrivateKeyFile(filePath));
  }

  @Test
  void setSaltFile_Found() throws FileNotFoundException {
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(File.class))).thenAnswer(invoke -> true);

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    File file = new File("/test/path/assumed/valid");
    parameters.setSaltFile(file);
    assertEquals(file.getPath(), parameters.getSaltFile().getPath());

    // Test with string parameter
    String filePath = file.getPath();
    parameters.setSaltFile(filePath);
    assertEquals(filePath, parameters.getSaltFile().getPath());
  }

  @Test
  void setSaltFile_NotFound() {
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(File.class))).thenAnswer(invoke -> false);

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    File file = new File("/test/path/assumed/invalid");
    assertThrows(FileNotFoundException.class, () -> parameters.setSaltFile(file));

    String filePath = "/test/path/assumed/invalid";
    assertThrows(FileNotFoundException.class, () -> parameters.setSaltFile(filePath));
  }

  @Test
  void setPatientFile_Found() throws FileNotFoundException {
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(File.class))).thenAnswer(invoke -> true);

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    File file = new File("/test/path/assumed/valid");
    parameters.setPatientFile(file);
    assertEquals(file.getPath(), parameters.getPatientFile().getPath());

    // Test with string parameter
    String filePath = file.getPath();
    parameters.setPatientFile(filePath);
    assertEquals(filePath, parameters.getPatientFile().getPath());
  }

  @Test
  void setPatientFile_NotFound() {
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(File.class))).thenAnswer(invoke -> false);

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    File file = new File("/test/path/assumed/invalid");
    assertThrows(FileNotFoundException.class, () -> parameters.setPatientFile(file));

    String filePath = "/test/path/assumed/invalid";
    assertThrows(FileNotFoundException.class, () -> parameters.setPatientFile(filePath));
  }

  @Test
  void setPatientFile_NullEmpty() throws FileNotFoundException {
    EngineParameters parameters = new EngineParameters();
    assertNull(parameters.getPatientFile());
    parameters.setPatientFile((File)null);
    assertNull(parameters.getPatientFile());
    parameters.setPatientFile((String)null);
    assertNull(parameters.getPatientFile());
    parameters.setPatientFile("");
    assertNull(parameters.getPatientFile());
  }

  @Test
  void setPrivateDate_Valid() throws ParseException {
    EngineParameters parameters = new EngineParameters();
    String dateString = "01/15/2012";
    LocalDate date = LocalDate.parse(dateString, EngineParameters.PrivateDateFormatter);
    parameters.setPrivateDate(dateString);
    assertEquals(parameters.getPrivateDate(), date);
    parameters.setPrivateDate(date);
    assertEquals(parameters.getPrivateDate(), date);
  }

  @Test
  void setPrivateDate_Invalid() {
    EngineParameters parameters = new EngineParameters();
    assertThrows(DateTimeParseException.class, () -> parameters.setPrivateDate("May 5, 2019"));
  }

  @Test
  void setPrivateDate_NullEmpty() throws ParseException {
    EngineParameters parameters = new EngineParameters();
    assertNull(parameters.getPrivateDate());
    parameters.setPrivateDate((LocalDate)null);
    assertNull(parameters.getPrivateDate());
    parameters.setPrivateDate((String)null);
    assertNull(parameters.getPrivateDate());
    parameters.setPrivateDate("");
    assertNull(parameters.getPrivateDate());
  }

  @Test
  void setOutputDirectory_Found() throws FileNotFoundException {
    String path = "/test/path/assumed/valid";
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(Path.class))).thenAnswer(invoke -> true);
    Mockito.when(fileHelperMock.pathFromString(Mockito.any(String.class))).thenAnswer(invoke -> Paths.get(path));

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    parameters.setOutputDirectory(path);
    assertEquals(Paths.get(path).toString(), parameters.getOutputDirectory().toString());
  }

  @Test
  void setOutputDirectory_NotFound() {
    String path = "/test/path/assumed/invalid";
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(Path.class))).thenAnswer(invoke -> false);
    Mockito.when(fileHelperMock.pathFromString(Mockito.any(String.class))).thenAnswer(invoke -> Paths.get(path));

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    assertThrows(FileNotFoundException.class, () -> parameters.setOutputDirectory(path));
  }

  @Test
  void setDelimiter_Default() {
    EngineParameters parameters = new EngineParameters();
    assertEquals(EngineParameters.DEFAULT_DELIMITER, parameters.getDelimiter());
  }

  @Test
  void setDelimiter_Invalid() {
    EngineParameters parameters = new EngineParameters();
    assertThrows(LinkjaException.class, () -> parameters.setDelimiter(null));
    assertEquals(EngineParameters.DEFAULT_DELIMITER, parameters.getDelimiter());
    assertThrows(LinkjaException.class, () -> parameters.setDelimiter(""));
    assertEquals(EngineParameters.DEFAULT_DELIMITER, parameters.getDelimiter());
    assertThrows(LinkjaException.class, () -> parameters.setDelimiter("  "));
    assertEquals(EngineParameters.DEFAULT_DELIMITER, parameters.getDelimiter());
    assertThrows(LinkjaException.class, () -> parameters.setDelimiter("||"));
    assertEquals(EngineParameters.DEFAULT_DELIMITER, parameters.getDelimiter());
  }

  @Test
  void setDelimiter_Valid() throws LinkjaException {
    EngineParameters parameters = new EngineParameters();
    parameters.setDelimiter(" | ");
    assertEquals('|', parameters.getDelimiter());
  }

  @Test
  void setNumWorkerThreads_Default() {
    EngineParameters parameters = new EngineParameters();
    assertEquals(EngineParameters.DEFAULT_WORKER_THREADS, parameters.getNumWorkerThreads());
  }

  @Test
  void setNumWorkerThreads_Conversion() {
    EngineParameters parameters = new EngineParameters();
    parameters.setNumWorkerThreads("12");
    assertEquals(12, parameters.getNumWorkerThreads());
    parameters.setNumWorkerThreads(" 13 ");
    assertEquals(13, parameters.getNumWorkerThreads());
  }

  @Test
  void setNumWorkerThreads_Invalid() {
    EngineParameters parameters = new EngineParameters();
    assertThrows(NumberFormatException.class, () -> parameters.setNumWorkerThreads("abc"));
    assertThrows(InvalidParameterException.class, () -> parameters.setNumWorkerThreads("0"));
    assertThrows(InvalidParameterException.class, () -> parameters.setNumWorkerThreads("-1"));
    assertThrows(InvalidParameterException.class, () -> parameters.setNumWorkerThreads(0));
    assertThrows(InvalidParameterException.class, () -> parameters.setNumWorkerThreads(-1));
    assertThrows(InvalidParameterException.class, () -> parameters.setNumWorkerThreads(null));
  }

  @Test
  void setBatchSize_Default() {
    EngineParameters parameters = new EngineParameters();
    assertEquals(EngineParameters.DEFAULT_BATCH_SIZE, parameters.getBatchSize());
  }

  @Test
  void setBatchSize_Conversion() {
    EngineParameters parameters = new EngineParameters();
    parameters.setBatchSize("1200");
    assertEquals(1200, parameters.getBatchSize());
    parameters.setBatchSize(" 130000000 ");
    assertEquals(130000000, parameters.getBatchSize());
  }

  @Test
  void setBatchSize_Invalid() {
    EngineParameters parameters = new EngineParameters();
    assertThrows(NumberFormatException.class, () -> parameters.setBatchSize("abc"));
    assertThrows(InvalidParameterException.class, () -> parameters.setBatchSize("0"));
    assertThrows(InvalidParameterException.class, () -> parameters.setBatchSize("-1"));
    assertThrows(InvalidParameterException.class, () -> parameters.setBatchSize(0));
    assertThrows(InvalidParameterException.class, () -> parameters.setBatchSize(-1));
    assertThrows(InvalidParameterException.class, () -> parameters.setBatchSize(null));
  }

  @Test
  void setRunNormalizationStep_Default() {
    EngineParameters parameters = new EngineParameters();
    assertEquals(EngineParameters.DEFAULT_RUN_NORMALIZATION_STEP, parameters.isRunNormalizationStep());
  }

  @Test
  void setRunNormalizationStep_Null() {
    EngineParameters parameters = new EngineParameters();
    parameters.setRunNormalizationStep(null);
    assertEquals(EngineParameters.DEFAULT_RUN_NORMALIZATION_STEP, parameters.isRunNormalizationStep());
  }

  @Test
  void setRunNormalizationStep_Conversion() {
    EngineParameters parameters = new EngineParameters();
    parameters.setRunNormalizationStep("true");
    assert(parameters.isRunNormalizationStep());
    parameters.setRunNormalizationStep("True");
    assert(parameters.isRunNormalizationStep());
    parameters.setRunNormalizationStep("false");
    assertFalse(parameters.isRunNormalizationStep());

    // This is the way boolean conversions work in Java - if it's not "true", it's false
    parameters.setRunNormalizationStep("blah");
    assertFalse(parameters.isRunNormalizationStep());
  }

  @Test
  void setWriteUnhashedData_Default() {
    EngineParameters parameters = new EngineParameters();
    assertEquals(EngineParameters.DEFAULT_WRITE_UNHASHED_DATA, parameters.isWriteUnhashedData());
  }

  @Test
  void setWriteUnhashedData_Null() {
    EngineParameters parameters = new EngineParameters();
    parameters.setRunNormalizationStep(null);
    assertEquals(EngineParameters.DEFAULT_WRITE_UNHASHED_DATA, parameters.isWriteUnhashedData());
  }

  @Test
  void setMinSaltLength_Default() {
    EngineParameters parameters = new EngineParameters();
    assertEquals(EngineParameters.DEFAULT_MIN_SALT_LENGTH, parameters.getMinSaltLength());
  }

  @Test
  void setMinSaltLength_Conversion() {
    EngineParameters parameters = new EngineParameters();
    parameters.setMinSaltLength("10");
    assertEquals(10, parameters.getMinSaltLength());
    parameters.setMinSaltLength(" 12 ");
    assertEquals(12, parameters.getMinSaltLength());
  }

  @Test
  void setMinSaltLength_Invalid() {
    EngineParameters parameters = new EngineParameters();
    assertThrows(NumberFormatException.class, () -> parameters.setMinSaltLength("abc"));
    assertThrows(InvalidParameterException.class, () -> parameters.setMinSaltLength("0"));
    assertThrows(InvalidParameterException.class, () -> parameters.setMinSaltLength("-1"));
    assertThrows(InvalidParameterException.class, () -> parameters.setMinSaltLength(0));
    assertThrows(InvalidParameterException.class, () -> parameters.setMinSaltLength(-1));
    assertThrows(InvalidParameterException.class, () -> parameters.setMinSaltLength(null));
  }

  @Test
  void setDisplaySaltContents_Default() {
    EngineParameters parameters = new EngineParameters();
    assertEquals(EngineParameters.DEFAULT_DISPLAY_SALT_MODE, parameters.isDisplaySaltMode());
  }

  @Test
  void setHashingMode_Default() {
    EngineParameters parameters = new EngineParameters();
    assertEquals(EngineParameters.DEFAULT_HASHING_MODE, parameters.isHashingMode());
  }

  @Test
  void hashingModeOptionsSet_Default() {
    EngineParameters parameters = new EngineParameters();
    assertFalse(parameters.hashingModeOptionsSet());
  }

  @Test
  void hashingModeOptionsSet_HashingNotEnabled() {
    EngineParameters parameters = new EngineParameters();
    parameters.setHashingMode(false);
    assertFalse(parameters.hashingModeOptionsSet());
  }

  @Test
  void hashingModeOptionsSet_IncrementalSet() throws FileNotFoundException, ParseException {
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(File.class))).thenAnswer(invoke -> true);

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    parameters.setHashingMode(true);
    File file = new File("/test/path/assumed/valid");
    parameters.setPrivateKeyFile(file);
    assertFalse(parameters.hashingModeOptionsSet());
    parameters.setSaltFile(file);
    assertFalse(parameters.hashingModeOptionsSet());
    parameters.setPatientFile(file);
    assertFalse(parameters.hashingModeOptionsSet());
    parameters.setPrivateDate("01/01/1900");
    // At this point both required parameters are set so it should be valid
    assertTrue(parameters.hashingModeOptionsSet());
  }

  @Test
  void displaySaltModeOptionsSet_Default() {
    EngineParameters parameters = new EngineParameters();
    assertFalse(parameters.displaySaltModeOptionsSet());
  }

  @Test
  void displaySaltModeOptionsSet_DisplaySaltNotEnabled() throws FileNotFoundException {
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(File.class))).thenAnswer(invoke -> true);

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    parameters.setDisplaySaltMode(false);
    assertFalse(parameters.displaySaltModeOptionsSet());

    // Even after setting the required parameters, it's not enabled because the mode is still turned off
    File file = new File("/test/path/assumed/valid");
    parameters.setPrivateKeyFile(file);
    parameters.setSaltFile(file);
    assertFalse(parameters.displaySaltModeOptionsSet());
  }

  @Test
  void displaySaltModeOptionsSet_IncrementalSet() throws FileNotFoundException {
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(File.class))).thenAnswer(invoke -> true);

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    parameters.setDisplaySaltMode(true);
    File file = new File("/test/path/assumed/valid");
    parameters.setPrivateKeyFile(file);
    assertFalse(parameters.displaySaltModeOptionsSet());
    parameters.setSaltFile(file);
    // At this point both required parameters are set so it should be valid
    assertTrue(parameters.displaySaltModeOptionsSet());
  }
}