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
  void patientIDHash() {
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
    row = step.patientIDHash(row);
    assertEquals("EF07138D9C0FBF62FD10CC2D88F35BACCD4CF9C6469F0A3A517D43DEEF5210B8D4C717C10E8D8C61DB37A79A1E43A67EE33D753663FCA047C5F950C9637D7669",
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    row = step.fnamelnamedobHash(row);
    assertEquals("B35CF0CA102F4F95B5E34DCCDB4941C203842D7B044A425F90CB355D06FCD6EAB830F05AA8141B212718406DD16C98CD8A09A6ACFA29C3BC8F25440F7E143AFA",
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    row = step.fnamelnamedobssnHash(row);
    assertEquals("08778A5FA5AD53C2C7AC217F4A5514070F418823863CF72E5DF0CEBFA5751097BFA654ED33A547C78F65BD42F0FEB7F21CE66C5124DABA4F573C64530ADB03CF",
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    row = step.lnamefnamedobssnHash(row);
    assertEquals("DC8886F8AC022FD2F50F1BDB3809934AF039A424C209D2F3CABA585C34527B7CEF6A35EC757040F7CC728C25FEC92A454E43486DF21B89419F02B951C861CCCE",
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    row = step.lnamefnamedobHash(row);
    assertEquals("30C0440EB1EA1C9648ABB6F5FF4F306BC0245FFCD62D9DE3359BEAA2A5540B7895B805DB596E09D90B7D788839511DCE14248F93C3346E4831A71A28B9DC67BC",
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    row = step.fnamelnameTdobHash(row);
    assertEquals("D81498A250E9324977A96637DE298745BCEB2619A0D5E31064327141BDED55CE515B0010B97F3C9C24EBF829DD1C1EA9D469A54D26899C6C767758D69612DEB8",
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    row = step.fnamelnameTdobssnHash(row);
    assertEquals("F5168C9AE158BEDD7AA458756F8A9D8C95F39667298BEDA96D00EFA2A066FB4D6662BA78E10C71F948C4C0269DC1DADDBECB896E698D9DAF1A941583DEEBEE8C",
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    row = step.fname3lnamedobssnHash(row);
    assertEquals("75732A5D6BC28C9B90718F5620D8EC9B620DB3B4A1669887A960B9EE7C81FB412590712A7A463919575CC7665596B199D5C4DDE50DB1754941A11C9E0991F2F3",
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    row = step.fname3lnamedobHash(row);
    assertEquals("D0FD381AB98B64A43684FE1ECE07ADD02B8F38FA1AFF910DD21EB2B553092C136F856EF45B4D119838B5AC3327118FF0D98D8A1ECD69A96B2B512CF3503E8DF9",
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    row = step.fnamelnamedobDssnHash(row);
    assertEquals("9828F423E1FAF9DD7213236A617D7084E9AD24CE6516887DF41D4C946EA3B736CCB0EDAB357B9B7C11E90E8BC866B411801A9B206FCEAA4F8264E857E42BE7F4",
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
    parameters.setPrivateSalt("9876543210987");
    parameters.setProjectSalt("0123456789123");

    HashingStep step = new HashingStep(parameters, fieldIds);
    step.cacheConvertedData(row);
    row = step.fnamelnamedobYssnHash(row);
    assertEquals("297528C0D7765E3823B81DECA6900A8CA4073D598CDF415285064B4DC26C3DC8151C16570344CA5DFD61B761D485317FDD5F18055C91B05765A5FDA102DF5AC5",
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

  private void assertNotNulLOrEmpty(String value) {
    assertNotNull(value);
    assertNotEquals("", value);
  }
}