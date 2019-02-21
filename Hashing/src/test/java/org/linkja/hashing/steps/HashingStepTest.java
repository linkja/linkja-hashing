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
    row.put(Engine.PATIENT_ID_FIELD, "12345");
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
}