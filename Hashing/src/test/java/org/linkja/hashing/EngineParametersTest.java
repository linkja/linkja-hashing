package org.linkja.hashing;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class EngineParametersTest {

  @Test
  void setPrivateKeyFile_Found() throws FileNotFoundException {
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(File.class))).thenAnswer(invoke -> true);

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    File file = new File("/test/path/assumed/valid");
    parameters.setPrivateKeyFile(file);
    assertEquals(parameters.getPrivateKeyFile().getPath(), file.getPath());

    String filePath = "/test/path/assumed/valid";
    parameters.setPrivateKeyFile(filePath);
    assertEquals(parameters.getPrivateKeyFile().getPath(), filePath);
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
    assertEquals(parameters.getSaltFile().getPath(), file.getPath());

    String filePath = "/test/path/assumed/valid";
    parameters.setSaltFile(filePath);
    assertEquals(parameters.getSaltFile().getPath(), filePath);
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
    assertEquals(parameters.getPatientFile().getPath(), file.getPath());

    String filePath = "/test/path/assumed/valid";
    parameters.setPatientFile(filePath);
    assertEquals(parameters.getPatientFile().getPath(), filePath);
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
  void setPrivateDate_Valid() throws ParseException {
    EngineParameters parameters = new EngineParameters();
    String dateString = "01/15/2012";
    Date date = new SimpleDateFormat("MM/dd/yyyy").parse(dateString);
    parameters.setPrivateDate(dateString);
    assertEquals(parameters.getPrivateDate(), date);
    parameters.setPrivateDate(date);
    assertEquals(parameters.getPrivateDate(), date);
  }

  @Test
  void setPrivateDate_Invalid() {
    EngineParameters parameters = new EngineParameters();
    assertThrows(ParseException.class, () -> parameters.setPrivateDate("May 5, 2019"));
  }

  @Test
  void setOutputDirectory_Found() throws FileNotFoundException {
    String path = "/test/path/assumed/valid";
    FileHelper fileHelperMock = Mockito.mock(FileHelper.class);
    Mockito.when(fileHelperMock.exists(Mockito.any(Path.class))).thenAnswer(invoke -> true);
    Mockito.when(fileHelperMock.pathFromString(Mockito.any(String.class))).thenAnswer(invoke -> Paths.get(path));

    EngineParameters parameters = new EngineParameters(fileHelperMock);
    parameters.setOutputDirectory(path);
    assertEquals(parameters.getOutputDirectory().toString(), path);
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
}