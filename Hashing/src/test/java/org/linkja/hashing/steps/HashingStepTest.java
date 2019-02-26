package org.linkja.hashing.steps;

import org.junit.jupiter.api.Test;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;
import org.linkja.hashing.HashParameters;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class HashingStepTest {

  @Test
  void run_ValidationNotRun() {
    DataRow row = new DataRow();
    HashingStep step = new HashingStep(null);
    row = step.run(row);
    assertEquals("The processing pipeline requires the Validation Filter to be run before Hashing.", row.getInvalidReason());
  }

  @Test
  void run_TracksCompletedStep() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    row = step.run(row);
    assert(row.hasCompletedStep(step.getStepName()));
  }

  @Test
  void run_SetsAllHashedFields() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5555");
    row.addCompletedStep(new ValidationFilterStep().getStepName());
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");
    parameters.setPrivateDate(LocalDate.parse("2018-05-05"));

    HashingStep step = new HashingStep(parameters);
    assertEquals(5, row.keySet().size());
    row = step.run(row);
    assertEquals(16, row.keySet().size());

    assertNotEquals("", row.get(HashingStep.PIDHASH_FIELD));
    assertNotEquals("", row.get(HashingStep.FNAMELNAMEDOBSSN_FIELD));
    assertNotEquals("", row.get(HashingStep.FNAMELNAMEDOB_FIELD));
    assertNotEquals("", row.get(HashingStep.LNAMEFNAMEDOBSSN_FIELD));
    assertNotEquals("", row.get(HashingStep.LNAMEFNAMEDOB_FIELD));
    assertNotEquals("", row.get(HashingStep.FNAMELNAMETDOBSSN_FIELD));
    assertNotEquals("", row.get(HashingStep.FNAMELNAMETDOB_FIELD));
    assertNotEquals("", row.get(HashingStep.FNAME3LNAMEDOBSSN_FIELD));
    assertNotEquals("", row.get(HashingStep.FNAME3LNAMEDOB_FIELD));
    assertNotEquals("", row.get(HashingStep.FNAMELNAMEDOBDSSN_FIELD));
    assertNotEquals("", row.get(HashingStep.FNAMELNAMEDOBYSSN_FIELD));
  }

  @Test
  void run_DoesNotTrackCompletedStepWhenInvalid() {
    DataRow row = new DataRow();
    row.setInvalidReason("Invalid for testing purposes");
    row.addCompletedStep(new ValidationFilterStep().getStepName());
    HashingStep step = new HashingStep(null);
    row = step.run(row);
    assertFalse(row.hasCompletedStep(step.getStepName()));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CONCAT('12345','3',datediff(dd,'1950-01-01','2000-01-01')),'0123456789123')  -- PIDHASH
  void patientIDHash() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-01-01");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");
    parameters.setSiteId("3");
    parameters.setPrivateDate(LocalDate.parse("2000-01-01"));

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.patientIDHash(row);
    assertEquals("E17144CF797141987527A3AED983D20D39C77A43A4B002E46667AD09C5C577DB66B258432802997AAD56E021C589139DE358B47FB71FB37551F18EF44077B2D1",
            row.get(HashingStep.PIDHASH_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE','1950-06-06') as varchar(max)),'0123456789123') -- fnamelnamedob
  void fnamelnamedobHash() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.fnamelnamedobHash(row);
    assertEquals("3C6F1E81D3BF29333AF148B0448E624C94E31EC8F4DC9A4B10F980870005B1695ACB146F24CEBB0E61850D2CCB2EDB1B107F70C58EA9C94C177698125BA04385",
            row.get(HashingStep.FNAMELNAMEDOB_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE','1950-06-06','5555') as varchar(max)),'0123456789123') -- fnamelnamedobssn
  void fnamelnamedobssnHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5555");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.fnamelnamedobssnHash(row);
    assertEquals("7D0064D9573467E13094FB81679985018B00939370EF831B6B4AAA0A0B1ECB0A773E229A76535EA80BEE0901E46D192D6C899EE3A9822F615AD7C5CE6520E9F4",
            row.get(HashingStep.FNAMELNAMEDOBSSN_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('DOE','JON','1950-06-06','5555') as varchar(max)),'0123456789123') -- lnamefnamedobssn
  void lnamefnamedobssnHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5555");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.lnamefnamedobssnHash(row);
    assertEquals("820502C524C52CA2884FA87901BA31B8956D19408A7839CD0986C042BC487B7043E96E45A287F84B7381042F01A37988806A10AA084F09760527300E302D53A4",
            row.get(HashingStep.LNAMEFNAMEDOBSSN_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('DOE','JON','1950-06-06') as varchar(max)),'0123456789123') -- lnamefnamedob
  void lnamefnamedobHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.lnamefnamedobHash(row);
    assertEquals("66F05558D03A8AC1F83C66B6F9AFD57FEA0E909DD5A49B59382D3C33E1C74459D6112BF616D9DB5CF3B9EF7AFCC605327BA38DB01FE7E35CC11FC0D9EEC2F96A",
            row.get(HashingStep.LNAMEFNAMEDOB_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE',dbo.fnFormatDate('1950-12-15','YYYY-DD-MM')) as varchar(max)),'0123456789123') -- fnamelnameTdob
  void fnamelnameTdobHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-12-15");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.fnamelnameTdobHash(row);
    assertEquals("C8D543DEF15CDACB54BB0D1A585DD3EF59CCBA22D38C230BD332F237029B2F4344A60C0B914B2E6FA16E2FCB4BFE5655CA2A93DBB9C7126B81129A2465447EF1",
            row.get(HashingStep.FNAMELNAMETDOB_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE',dbo.fnFormatDate('1950-12-15','YYYY-DD-MM'), '5555') as varchar(max)),'0123456789123') -- fnamelnameTdobssn
  void fnamelnameTdobssnHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-12-15");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5555");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.fnamelnameTdobssnHash(row);
    assertEquals("90674FDBBE3DEECBD122D3924343C049345977B040FB6232F84061D2618BA1147A88B57693E80F81EF09C4EEA437446973D9A9C92D9C07E7199A21F499FB8DFE",
            row.get(HashingStep.FNAMELNAMETDOBSSN_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT(substring('JONATHAN',1,3),'DOUGH','1950-06-06','5555') as varchar(max)),'0123456789123') -- fname3lnamedobssn
  void fname3lnamedobssnHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JONATHAN");
    row.put(Engine.LAST_NAME_FIELD, "DOUGH");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5555");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.fname3lnamedobssnHash(row);
    assertEquals("A49C5EBAB51AF1369DD8BA3B26D9636C2063DB9AF33AFA25C4262E38DF5A65E57FEFD3DC2E3B3F4291FDD283FF8A78FB9D5164A6033528ECB2AF6A7CA78A7D2B",
            row.get(HashingStep.FNAME3LNAMEDOBSSN_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT(substring('JONATHAN',1,3),'DOUGH','1950-06-06') as varchar(max)),'0123456789123')   -- fname3lnamedob
  void fnamelname3dobHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JONATHAN");
    row.put(Engine.LAST_NAME_FIELD, "DOUGH");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.fname3lnamedobHash(row);
    assertEquals("D6F6362ED6EDCB1C94C48109B0130F382DF5AAE1CA8B0016FE9967EF5C17259AB3027B94099EA0C3BF9C21F2DED6EAF78A814005609C13FD54F05C1F300C62A8",
            row.get(HashingStep.FNAME3LNAMEDOB_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE',dateadd(dd,1,cast('1950-06-06' as date)),'5555') as varchar(max)),'0123456789123') -- fnamelnamedobDssn
  void fnamelnamedobDssnHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5555");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.fnamelnamedobDssnHash(row);
    assertEquals("EB6C3AAE47182D00F0DF54CA580D5B5DB3F05D3F19C936385F43960DD63BCD742CD61B16A894B2DABDA2A47ED2C81D7E310117893EF86806F7BD83002B291AF1",
            row.get(HashingStep.FNAMELNAMEDOBDSSN_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE',dateadd(YYYY,1,cast('1950-06-06' as date)),'5555') as varchar(max)),'0123456789123') -- fnamelnamedobYssn
  void fnamelnamedobYssnHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5555");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("0123456789123");

    HashingStep step = new HashingStep(parameters);
    step.cacheConvertedData(row);
    row = step.fnamelnamedobYssnHash(row);
    assertEquals("2240E9DB53B395BBFDF921D127E5B6819A0FDEA3DDC4011813393D56A1DE5BE24A6D2B812FE9D69BE3A5D05C519E5668090434EFD7DA94F834DEF216735E0A4F",
            row.get(HashingStep.FNAMELNAMEDOBYSSN_FIELD));
  }

  @Test
  void safeSubstring_NullEmpty() {
    assertNull(HashingStep.safeSubstring(null, 0, 3));
    assertEquals("", HashingStep.safeSubstring("", 0, 3));
  }

  @Test
  void safeSubstring_EmptyRange() {
    assertEquals("", HashingStep.safeSubstring("Test", 0, 0));
  }

  @Test
  void safeSubstring_ValidRanges() {
    assertEquals("Te", HashingStep.safeSubstring("Test", 0, 2));
    assertEquals("es", HashingStep.safeSubstring("Test", 1, 2));
    assertEquals("st", HashingStep.safeSubstring("Test", 2, 2));
  }

  @Test
  void safeSubstring_LongRanges() {
    assertEquals("Test", HashingStep.safeSubstring("Test", 0, 500));
    assertEquals("st", HashingStep.safeSubstring("Test", 2, 5));
  }
}