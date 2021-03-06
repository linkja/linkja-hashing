package org.linkja.hashing.steps;

import org.junit.jupiter.api.Test;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;
import org.linkja.hashing.HashParameters;

import java.time.LocalDate;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class HashingStepTest {

  private static HashMap<String, String> fieldIds = new HashMap<String, String>() {{
    put("first_name", "FN");
    put("last_name", "LN");
    put("date_of_birth", "DOB");
    put("social_security_number", "SSN");
    put("patient_id", "PID");
  }};

  @Test
  void run_ValidationNotRun() {
    DataRow row = new DataRow();
    HashingStep step = new HashingStep(null, fieldIds);
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
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");
    parameters.setPrivateDate(LocalDate.parse("2018-05-05"));

    HashingStep step = new HashingStep(parameters, fieldIds);
    assertEquals(5, row.keySet().size());
    row = step.run(row);
    assertEquals(16, row.keySet().size());

    assertNotNulLOrEmpty(row.get(HashingStep.PIDHASH_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAMELNAMEDOBSSN_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAMELNAMEDOB_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.LNAMEFNAMEDOBSSN_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.LNAMEFNAMEDOB_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAMELNAMETDOBSSN_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAMELNAMETDOB_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAME3LNAMEDOBSSN_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAME3LNAMEDOB_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAMELNAMEDOBDSSN_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAMELNAMEDOBYSSN_FIELD));
  }

  @Test
  void run_SetsAllHashedFieldsNonSSN() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-06-06");
    row.addCompletedStep(new ValidationFilterStep().getStepName());
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");
    parameters.setPrivateDate(LocalDate.parse("2018-05-05"));

    HashingStep step = new HashingStep(parameters, fieldIds);
    assertEquals(4, row.keySet().size());
    row = step.run(row);
    assertEquals(9, row.keySet().size());

    assertNotNulLOrEmpty(row.get(HashingStep.PIDHASH_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAMELNAMEDOB_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.LNAMEFNAMEDOB_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAMELNAMETDOB_FIELD));
    assertNotNulLOrEmpty(row.get(HashingStep.FNAME3LNAMEDOB_FIELD));

    assertNull(row.get(HashingStep.FNAMELNAMEDOBSSN_FIELD));
    assertNull(row.get(HashingStep.LNAMEFNAMEDOBSSN_FIELD));
    assertNull(row.get(HashingStep.FNAMELNAMETDOBSSN_FIELD));
    assertNull(row.get(HashingStep.FNAME3LNAMEDOBSSN_FIELD));
    assertNull(row.get(HashingStep.FNAMELNAMEDOBDSSN_FIELD));
    assertNull(row.get(HashingStep.FNAMELNAMEDOBYSSN_FIELD));
  }

  @Test
  void run_ProcessesAllDerivedRows() {
    DataRow row = createRow("12345", "JON", "DOESMITH", "1950-06-06", "5555", true);
    row.addDerivedRow(createRow("12345", "JON", "DOE", "1950-06-06", "5555", true));
    row.addDerivedRow(createRow("12345", "JON", "SMITH", "1950-06-06", "5555", true));
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");
    parameters.setPrivateDate(LocalDate.parse("2018-05-05"));

    HashingStep step = new HashingStep(parameters, fieldIds);
    row = step.run(row);

    for (DataRow derivedRow : row.getDerivedRows()) {
      assertNotNulLOrEmpty(derivedRow.get(HashingStep.PIDHASH_FIELD));
      assertNotNulLOrEmpty(derivedRow.get(HashingStep.FNAMELNAMEDOBSSN_FIELD));
      assertNotNulLOrEmpty(derivedRow.get(HashingStep.FNAMELNAMEDOB_FIELD));
      assertNotNulLOrEmpty(derivedRow.get(HashingStep.LNAMEFNAMEDOBSSN_FIELD));
      assertNotNulLOrEmpty(derivedRow.get(HashingStep.LNAMEFNAMEDOB_FIELD));
      assertNotNulLOrEmpty(derivedRow.get(HashingStep.FNAMELNAMETDOBSSN_FIELD));
      assertNotNulLOrEmpty(derivedRow.get(HashingStep.FNAMELNAMETDOB_FIELD));
      assertNotNulLOrEmpty(derivedRow.get(HashingStep.FNAMELNAMEDOBDSSN_FIELD));
      assertNotNulLOrEmpty(derivedRow.get(HashingStep.FNAMELNAMEDOBYSSN_FIELD));

      // Derived rows should not have the substring hash calculated
      assertNull(derivedRow.get(HashingStep.FNAME3LNAMEDOBSSN_FIELD));
      assertNull(derivedRow.get(HashingStep.FNAME3LNAMEDOB_FIELD));
    }
  }

  @Test
  void run_DoesNotTrackCompletedStepWhenInvalid() {
    DataRow row = new DataRow();
    row.setInvalidReason("Invalid for testing purposes");
    row.addCompletedStep(new ValidationFilterStep().getStepName());
    HashingStep step = new HashingStep(null, fieldIds);
    row = step.run(row);
    assertFalse(row.hasCompletedStep(step.getStepName()));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CONCAT('12345','3',datediff(dd,'1950-01-01','2000-01-01')),'9876543210987')  -- PIDHASH
  void patientIDStringAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-01-01");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");
    parameters.setSiteId("3");
    parameters.setPrivateDate(LocalDate.parse("2000-01-01"));

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("PID12345SID3DOF18262PVS9876543210987", step.patientIDString(row));
    row = step.patientIDHash(row);
    assertEquals("7FA89170C43208C29EF4ABE50373AFE3EC577CDFA3118F46955FB266C17915C0",
    //SHA-512: assertEquals("EF07138D9C0FBF62FD10CC2D88F35BACCD4CF9C6469F0A3A517D43DEEF5210B8D4C717C10E8D8C61DB37A79A1E43A67EE33D753663FCA047C5F950C9637D7669",
            row.get(HashingStep.PIDHASH_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE','1950-07-06') as varchar(max)),'0123456789123') -- fnamelnamedob
  void fnamelnamedobStringAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-07-06");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("FNJONLNDOEDOB1950-07-06PRS0123456789123", step.fnamelnamedobString(row));
    row = step.fnamelnamedobHash(row);
    assertEquals("176C55BB94C43918D6CEEFE77C4E1B45EB162ABD33194EACA15E96AF75C36280",
    //SHA-512: assertEquals("A1953D96314C5B48B2CF274910DA2A9B542A5457E552F63574D13989DC236A884ECF2CAF4D229EDB7313B38B881B4CB441A24B7517A6BB98070BE0C4ADC9266F",
            row.get(HashingStep.FNAMELNAMEDOB_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE','1950-07-06','5678') as varchar(max)),'0123456789123') -- fnamelnamedobssn
  void fnamelnamedobssnStringAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-07-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5678");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("FNJONLNDOEDOB1950-07-06SSN5678PRS0123456789123", step.fnamelnamedobssnString(row));
    row = step.fnamelnamedobssnHash(row);
    assertEquals("93C47F2FF4B02EA852B5AF19FA2793774E24EDD75EBEB161BBA0307DBE06AB01",
    //SHA-512: assertEquals("8C74329D89AD668297B624C609A5F4B43F4C4B715495360D34BA1B9F01FE90E05887AF81F3F02052BC9BF554854566752ADB1F136D5B1798F8AF8240356A96BE",
            row.get(HashingStep.FNAMELNAMEDOBSSN_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('DOE','JON','1950-07-06','5678') as varchar(max)),'0123456789123') -- lnamefnamedobssn
  void lnamefnamedobssnStringAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-07-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5678");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("FNDOELNJONDOB1950-07-06SSN5678PRS0123456789123", step.lnamefnamedobssnString(row));
    row = step.lnamefnamedobssnHash(row);
    assertEquals("0924C83CF59F4EB1C9440579ABE6D2AB644BF4A3B4508BAEEF8EB8CD6666A862",
    //SHA-512: assertEquals("D433E574BC796869453EC88E6A1CF0662F6E4725F65638FFBE81BE01ADFCF2FC2D7B0670C433AFDAB172CE17F1D5DB1677795A7305E1B4EAF4C4350CFCD399F2",
            row.get(HashingStep.LNAMEFNAMEDOBSSN_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('DOE','JON','1950-07-06') as varchar(max)),'0123456789123') -- lnamefnamedob
  void lnamefnamedobStringAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-07-06");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("FNDOELNJONDOB1950-07-06PRS0123456789123", step.lnamefnamedobString(row));
    row = step.lnamefnamedobHash(row);
    assertEquals("58E0E49A037B32152A10948F8BBC82AE7E8DDCC9A1949D030AC922C765D39F6E",
    //SHA-512: assertEquals("BCCC7BD1F076520FA1F8E1EFA46F279003E0C9371DEE6B482366D68C9B5A3C2C356D5D7BC2AB89604694608A880AA06E531002D0F2A43665D1128445B9C92F88",
            row.get(HashingStep.LNAMEFNAMEDOB_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE',dbo.fnFormatDate('1950-12-15','YYYY-DD-MM')) as varchar(max)),'0123456789123') -- fnamelnameTdob
  void fnamelnameTdobStringAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-12-15");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("FNJONLNDOEDOB1950-15-12PRS0123456789123", step.fnamelnameTdobString(row));
    row = step.fnamelnameTdobHash(row);
    assertEquals("C64604ECD6E0D5AA9370CCC34354EC1A8EBC8307164886377297D3D2ED3B0F37",
    //SHA-512: assertEquals("D81498A250E9324977A96637DE298745BCEB2619A0D5E31064327141BDED55CE515B0010B97F3C9C24EBF829DD1C1EA9D469A54D26899C6C767758D69612DEB8",
            row.get(HashingStep.FNAMELNAMETDOB_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE',dbo.fnFormatDate('1950-12-15','YYYY-DD-MM'), '5678') as varchar(max)),'0123456789123') -- fnamelnameTdobssn
  void fnamelnameTdobssnStringAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-12-15");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5678");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("FNJONLNDOEDOB1950-15-12SSN5678PRS0123456789123", step.fnamelnameTdobssnString(row));
    row = step.fnamelnameTdobssnHash(row);
    assertEquals("F9041232BB11E995FF9BEA0A5995474BB045FA0C3067DBE7CFF3AE9AA3A7F01A",
    //SHA-512: assertEquals("2D9B2DBB074E557E9A573F892BCA53A9D7A30123B879FEE73940A44C402686F01CC5B7FBC1937DF3CC6B39917B51D383B9622C3A11A9B0D1F616E638F9A3A8D1",
            row.get(HashingStep.FNAMELNAMETDOBSSN_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT(substring('JONATHAN',1,3),'DOUGH','1950-07-06','5678') as varchar(max)),'0123456789123') -- fname3lnamedobssn
  void fname3lnamedobssnStringAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JONATHAN");
    row.put(Engine.LAST_NAME_FIELD, "DOUGH");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-07-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5678");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("FNJONLNDOUGHDOB1950-07-06SSN5678PRS0123456789123", step.fname3lnamedobssnString(row));
    row = step.fname3lnamedobssnHash(row);
    assertEquals("D3D5F9A135C314E467604AF40A99898E96E81DFC12E7E9CA118C26287E910196",
    //SHA-512: assertEquals("0B2A8F39E1B505A6119951442ECF90A816BC611A359855619779C5734709B9A33B246769D69D4D82C3C1A4D69E503D2684DF27E71D5DDD5E189B5E232D478B7E",
            row.get(HashingStep.FNAME3LNAMEDOBSSN_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT(substring('JONATHAN',1,3),'DOUGH','1950-07-06') as varchar(max)),'0123456789123')   -- fname3lnamedob
  void fname3lnamedobHashAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JONATHAN");
    row.put(Engine.LAST_NAME_FIELD, "DOUGH");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-07-06");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("FNJONLNDOUGHDOB1950-07-06PRS0123456789123", step.fname3lnamedobString(row));
    row = step.fname3lnamedobHash(row);
    assertEquals("646478B98CEF27AED6BD85AC9ED4C709D549EB8A227604E40EAB6B9066EFA56F",
    //SHA-512: assertEquals("978E208B39306F0854074AAF6D113CD9DE082B96D9190A29B5A6D0661A45388A7324F2113626585078CF1004DB98B4E9A2DC63E1BECEDCC4BF42BA0A72E0D6C5",
            row.get(HashingStep.FNAME3LNAMEDOB_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE',dateadd(dd,1,cast('1950-08-06' as date)),'5555') as varchar(max)),'0123456789123') -- fnamelnamedobDssn
  void fnamelnamedobDssnStringAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-08-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5555");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("FNJONLNDOEDOB1950-08-07SSN5555PRS0123456789123", step.fnamelnamedobDssnString(row));
    row = step.fnamelnamedobDssnHash(row);
    assertEquals("2F1E4B75CB8C7D59A5847493938F73C6D5D194BB5A46D9DE531599E26C29662F",
    //SHA-512: assertEquals("30E4590805B75C0F61368918DD01F47F2A9561B5AD7561ABDCC7EFEB7B00D0771E910467A971B45E3858205DA9F426832895C22EA8B37AC545B35674A280596B",
            row.get(HashingStep.FNAMELNAMEDOBDSSN_FIELD));
  }

  @Test
  // SQL Equivalent:
  // SELECT dbo.fnHashBytes2(CAST(CONCAT('JON','DOE',dateadd(YYYY,1,cast('1950-07-06' as date)),'5678') as varchar(max)),'0123456789123') -- fnamelnamedobYssn
  void fnamelnamedobYssnStringAndHash() {
    DataRow row = new DataRow();
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "1950-07-06");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "5678");
    HashParameters parameters = new HashParameters();
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    assertEquals("FNJONLNDOEDOB1951-07-06SSN5678PRS0123456789123", step.fnamelnamedobYssnString(row));
    row = step.fnamelnamedobYssnHash(row);
    assertEquals("7100640859CECAE5B5AD47765EDC795EA867A9D5630A5CA2E4AF5F9125A30B7E",
    //SHA-512: assertEquals("14DDF61465099F4BC5CBE02594C48DF512603DB34B3D21775BDD46ABBE1D1FCDA6D3613436A864743A7864CF8AD79CB65AC79C93C89FFF0EC995EA1CFA120516",
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

  private DataRow createRow(String patientID, String firstName, String lastName, String dob, String ssn, boolean isValidated) {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, patientID);
    row.put(Engine.FIRST_NAME_FIELD, firstName);
    row.put(Engine.LAST_NAME_FIELD, lastName);
    row.put(Engine.DATE_OF_BIRTH_FIELD, dob);
    row.put(Engine.SOCIAL_SECURITY_NUMBER, ssn);
    if (isValidated) {
      row.addCompletedStep(new ValidationFilterStep().getStepName());
    }
    return row;
  }

  private void assertNotNulLOrEmpty(Object value) {
    assertNotNull(value);
    assertNotEquals("", (String)value);
  }
}