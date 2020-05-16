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
    assertEquals("41299F409E2015868B7F4A6FCCF7FB9366F4F8960AB2DC48F56E6BE3360AF06F16A49F9492DA04D49CDA329B0E83652EC6FA8C8C999BAA190A4249DA68664E61",
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
    assertEquals("F57057F4506D03DF9249674FCDF1D5A2BDA4F17C859F19A566FA6156942A412430AC0CB315C3CC523C404629586D3FD14C0B0493B611C6084BD9FFE61A6BA682",
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
    assertEquals("F71B1874AD662FEF3DF4CBE7D898FA5136CB6B28F45AA07F37111B45582EC844B11AEE604944412D55040AF9F360B0C07A536B4226231D71DDC2E3BE06790EA0",
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
    assertEquals("082732F202BA86C16858AC415B2977E218FD7AD68807B1D7DEB1D1D60B84959E8FF0FCB4D9D997D31F09404C1FD6254D547A0D3B98632339329CBF2C2DBD347A",
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
    assertEquals("F1F313FFFABBE58A6B1261765A8250713BA031A6865795B008788EF0623AD814BFC810C636A532CA2A7C4A5111621F4EBAFD7BE598A191B62422773AA5D51D90",
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
    assertEquals("80A12218B086AE28BA86A3170C0381B6C168EB073EAB549462044A88BC77A3095B9507FFDE099515DB709255D7E93AEBA4F2334CA023276EE3AA8159A5187E66",
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
    assertEquals("A79E0F043B6468A0BAE1474E2015689E8602EBDF9A54A8ECAD5B888B84492CF8964B2CACE0826989E24335CC462FDAE4972BEA608CEEF5AFC6D312B280B90728",
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
    assertEquals("63C5C250769DCD54A947B4504C8D3FC143E3C2BA008D028C5BC7A3FC0D9E1F9C31F41EEA695B17222AECD13693F1630F67EAFBAF2C40A22EC6737F33714B539B",
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
    assertEquals("570864412E6325C0BB61F26483E91B9D87EB8DD279919FB2D8935C8814094E65BF2C8FBAB08A64244CA38F35C657EF35E87A2A8610F1A2B72DF991C440BFD84E",
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
    assertEquals("CC09A7C2A811E72093F78737C25417FF938A2292992401D8AF08E513B35FDFD90F0F168A97A988308FDA0FA39F13D8D5B771A38D8BC44D6A7D464832C481D4D7",
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
    assertEquals("C56C7D37EC85598424B8952392A3CB61FFC22367026C81E44E12B8F1AADD2F6C897BBDADC31718549B071E02C6274886A136D59CE1B26312606E27630B8CDE61",
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