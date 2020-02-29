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
    assertEquals("0B8D704973A94A60380AB30B699C208B9D0E3B185741F71F09D577623951AC9A8FAF9137315B18F71C17A7B0C117251CF42D48E351A3268D95BC436A92FC40F3",
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
    assertEquals("D36D613E1CE11070344EAA663563F3CD636E6DD932B02D108D9AAF8AD859683006E4A866AFF04E3FFB44FA9BEE618427F4D25AF1E9A8F8F9E7731E283A17DB6A",
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
    assertEquals("DBAA46E19D37446F93716575FEB1A232B66E1ADEF11C4CF96AB15D9780BABDAEF0049B625DCA21B1B85674D6C42B9ECC5384D201E6C6338671E4F10CB53EBA36",
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
    assertEquals("395130E08B6E034F6770F4EB488BB70E0C3C18E120D54405D36B066532008E7B56CD16C4C1BC676A99323EFC8EB9BC4ACDA4CA3A05D929DBEE8EF26FD01D85CA",
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
    assertEquals("BBDFA5D2CA30FCC76F636B1492DFF8AE9024389E09FD2459412CCEF840178BF74FDD5F4AC26EDCFED124826E4CC1287A612166F1DEB4F675511C7790B923B355",
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
    assertEquals("E76F8D1EBACCDB0E4E6F2D4FD7D1F0B45F6BF74099DA456C54B08C1284A09405F5E28464019D6703E9BC867A10D6F38556B278C2F836AF8DB09331EC03690F36",
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
    assertEquals("5313D7CAA50D858C95A626B9CE379E2DDB90182AC01180CEB2657D3AFB4E3D826648EAA48AAFC2A9C27AB447F72940F8ABF51AAD987F7A998399B7F5AC8044AF",
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
    assertEquals("46D4419C596975CCC227ABA8A42C7FF1D7D73BA1772A21C3AAC68E23CF3EBEBD7E1E1A64E5B26EE47928515C6FE4C238FE0D0AF4AF53529B064DF2806C5A22A9",
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
    assertEquals("1D5AAF2096FAD3F709CA3C5781C4C7655719EB24A53228038E1A1183A48F58BC469F7E37FCCB2B66CBF0D9912ECCA712057A35E0C130F1A40A2F54C68B2A10D0",
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
    assertEquals("F632497F76BDF33B0EB6922BDC87083C1FD3B38A0B16FCC46777AD58255714A2072AF3A98053D184B2DE624ADDD2D2997A5E6D7440A32CCD34E6A0BEE404AA9E",
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
    assertEquals("CA4591DAB32202DF65114DBBBF6A647A300E3F31266B137720E1D823368EA8E7BF1AF3750D05DA5008FF1922B9EA854172D2FC0F7B4C44371035487C29515F3F",
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