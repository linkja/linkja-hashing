package org.linkja.hashing.steps;

import org.junit.jupiter.api.Test;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

import static org.junit.jupiter.api.Assertions.*;

class ValidationFilterStepTest {

  @Test
  void checkRequiredFields_Null() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertNull(step.checkRequiredFields(null));
  }

  @Test
  void checkRequiredFields_Empty() {
    DataRow row = new DataRow();
    ValidationFilterStep step = new ValidationFilterStep();
    assertEquals(row, step.checkRequiredFields(row));
  }

  @Test
  void checkRequiredFields_Missing() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "");
    row.put(Engine.FIRST_NAME_FIELD, "  ");
    row.put(Engine.LAST_NAME_FIELD, "\t");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.checkRequiredFields(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Patient Identifier, First Name, Last Name, Date of Birth",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.PATIENT_ID_FIELD, "1");
    row = step.checkRequiredFields(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: First Name, Last Name, Date of Birth",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.LAST_NAME_FIELD, "Test");
    row = step.checkRequiredFields(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: First Name, Date of Birth",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.FIRST_NAME_FIELD, "Patient");
    row = step.checkRequiredFields(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Date of Birth",
            row.getInvalidReason());
  }

  @Test
  void checkRequiredFields_Filled() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "1");
    row.put(Engine.FIRST_NAME_FIELD, "Patient");
    row.put(Engine.LAST_NAME_FIELD, "Test");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "05/15/2000");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.checkRequiredFields(row);
    assertNull(row.getInvalidReason());
  }

  @Test
  void checkFieldLength_Null() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertNull(step.checkFieldLength(null));
  }

  @Test
  void checkFieldLength_Empty() {
    DataRow row = new DataRow();
    ValidationFilterStep step = new ValidationFilterStep();
    assertEquals(row, step.checkFieldLength(row));
  }

  @Test
  void checkFieldLength_TooShort() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "A");
    row.put(Engine.LAST_NAME_FIELD, "B");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "12/12/1912");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.checkFieldLength(row);
    assertEquals("The following fields must be longer than 1 character: First Name, Last Name",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.LAST_NAME_FIELD, "AB");
    row = step.checkFieldLength(row);
    assertEquals("The following fields must be longer than 1 character: First Name",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.FIRST_NAME_FIELD, "AB");
    row.put(Engine.LAST_NAME_FIELD, "B");
    row = step.checkFieldLength(row);
    assertEquals("The following fields must be longer than 1 character: Last Name",
            row.getInvalidReason());
  }

  @Test
  void checkFieldFormat_Null() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertNull(step.checkFieldFormat(null));
  }

  @Test
  void checkFieldFormat_Empty() {
    DataRow row = new DataRow();
    ValidationFilterStep step = new ValidationFilterStep();
    assertEquals(row, step.checkFieldFormat(row));
  }

  @Test
  void checkFieldFormat_InvalidFormat() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "asdf");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "333-3E-3333");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.checkFieldFormat(row);
    assertEquals("The following fields are not in a valid format: Date of Birth (recommended to use MM/DD/YYYY format), Social Security Number",
            row.getInvalidReason());
  }

  @Test
  void checkFieldFormat_ValidFormat() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "12/12/1912");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "333-43-5333");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.checkFieldFormat(row);
    assertNull(row.getInvalidReason());

    // Try different formats of date
    row.setInvalidReason(null);
    row.put(Engine.DATE_OF_BIRTH_FIELD, "   1/17/1950   ");
    row = step.checkFieldFormat(row);
    assertNull(row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.DATE_OF_BIRTH_FIELD, "   19500117   ");
    row = step.checkFieldFormat(row);
    assertNull(row.getInvalidReason());

    // We also make sure that things that may seem invalid are considered valid - this includes all whitespace or empty
    // SSN entries.
    row.setInvalidReason(null);
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "");
    row = step.checkFieldFormat(row);
    assertNull(row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "    ");
    row = step.checkFieldFormat(row);
    assertNull(row.getInvalidReason());
  }

  @Test
  void run_MultipleErrorTypes() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "");
    row.put(Engine.FIRST_NAME_FIELD, "A");
    row.put(Engine.LAST_NAME_FIELD, "B");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "asdf");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.run(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Patient Identifier, Date of Birth\r\nThe following fields must be longer than 1 character: First Name, Last Name\r\nThe following fields are not in a valid format: Date of Birth (recommended to use MM/DD/YYYY format), Social Security Number",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.FIRST_NAME_FIELD, "J1");
    row.put(Engine.LAST_NAME_FIELD, "D2");
    row = step.run(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Patient Identifier, Date of Birth\r\nThe following fields are not in a valid format: Date of Birth (recommended to use MM/DD/YYYY format), Social Security Number, First Name, Last Name",
      row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row = step.run(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Patient Identifier, Date of Birth\r\nThe following fields are not in a valid format: Date of Birth (recommended to use MM/DD/YYYY format), Social Security Number",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "A");
    row.put(Engine.LAST_NAME_FIELD, "B");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "12/12/asdf");
    row = step.run(row);
    assertEquals("The following fields must be longer than 1 character: First Name, Last Name\r\nThe following fields are not in a valid format: Date of Birth (recommended to use MM/DD/YYYY format)",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.PATIENT_ID_FIELD, "");
    row.put(Engine.FIRST_NAME_FIELD, "A");
    row.put(Engine.LAST_NAME_FIELD, "B");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "12/12/1912");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "3456");
    row = step.run(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Patient Identifier\r\nThe following fields must be longer than 1 character: First Name, Last Name",
            row.getInvalidReason());
  }

  @Test
  void run_SkipsChecksIfFlaggedNotToProcess() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "");
    row.put(Engine.FIRST_NAME_FIELD, "  ");
    row.put(Engine.LAST_NAME_FIELD, "\t");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "");
    row.setInvalidReason("Already invalid");

    ValidationFilterStep step = new ValidationFilterStep();

    // We're making sure that we don't perform the checks (which would update the invalid reason for this data row) because
    // the row is already marked as invalid.
    row = step.run(row);
    assertEquals("Already invalid", row.getInvalidReason());
  }

  @Test
  void run_FilledValid() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "1");
    row.put(Engine.FIRST_NAME_FIELD, "Patient");
    row.put(Engine.LAST_NAME_FIELD, "Test");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "05/15/2000");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.run(row);
    assertNull(row.getInvalidReason());

    row.put(Engine.SOCIAL_SECURITY_NUMBER, "333-33 4333");
    row = step.run(row);
    assertNull(row.getInvalidReason());
  }

  @Test
  void run_TracksCompletedStep() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "1");
    row.put(Engine.FIRST_NAME_FIELD, "Patient");
    row.put(Engine.LAST_NAME_FIELD, "Test");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "05/15/2000");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.run(row);
    assert(row.hasCompletedStep(step.getStepName()));
  }

  @Test
  void run_DoesNotTrackCompletedStepWhenInvalid() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "1");
    row.put(Engine.FIRST_NAME_FIELD, "Patient");
    row.put(Engine.LAST_NAME_FIELD, "Test");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "05/15/2000");
    row.setInvalidReason("Invalid for testing purposes");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.run(row);
    assertFalse(row.hasCompletedStep(step.getStepName()));
  }

  @Test
  void isValidDateFormat_NullEmpty() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidDateFormat(null));
    assertFalse(step.isValidDateFormat(""));
    assertFalse(step.isValidDateFormat("   "));
  }

  @Test
  void isValidDateFormat_DateValid() {
    ValidationFilterStep step = new ValidationFilterStep();
    assert(step.isValidDateFormat("11/05/2019"));  // MM/dd/yyyy
    assert(step.isValidDateFormat("3/2/2019"));    // M/d/yyyy
    assert(step.isValidDateFormat("3-2-2019"));    // M-d-yyyy
    assert(step.isValidDateFormat("2019-3-2"));    // yyyy-M-d
    assert(step.isValidDateFormat("2019/3/2"));    // yyyy/M/d
    assert(step.isValidDateFormat("2019 3 2"));    // yyyy M d
    assert(step.isValidDateFormat("20190302"));    // yyyyMMdd
    assert(step.isValidDateFormat("02/29/2016"));      // Valid leap year
  }

  @Test
  void isValidDateFormat_DateTimeValid() {
    ValidationFilterStep step = new ValidationFilterStep();
    assert(step.isValidDateFormat("11/05/2019 3:30"));      // MM/dd/yyyy H:mm
    assert(step.isValidDateFormat("3/2/2019 15:30"));       // M/d/yyyy HH:mm
    assert(step.isValidDateFormat("3-2-2019 03:30"));       // M-d-yyyy HH:mm
    assert(step.isValidDateFormat("2019-3-2 07:15:32"));    // yyyy-M-d HH:mm:ss
    assert(step.isValidDateFormat("7/4/1930 0:00"));        // M/d/yyyy H:mm
    assert(step.isValidDateFormat("02/29/2016 00:00"));     // Valid leap year
  }

  @Test
  void isValidDateFormat_DateInvalid() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidDateFormat("Nov 05 2019"));     // Not a format we support
    assertFalse(step.isValidDateFormat("05/2019"));         // Missing one of the elements
    assertFalse(step.isValidDateFormat("02/29/2019"));      // Not a valid leap year
    assertFalse(step.isValidDateFormat("4/35/2019"));       // Not a valid day in April
    assertFalse(step.isValidDateFormat("14/22/2019"));      // Not a valid month
  }

  @Test
  void isValidDateFormat_DateTimeInvalid() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidDateFormat("11/05/2019 25:15")); // Not a valid time
    assertFalse(step.isValidDateFormat("05/05/2019 5:5:5")); // Not a valid format
    assertFalse(step.isValidDateFormat("02/29/2019 05:15")); // Not a valid leap year
  }

  @Test
  void isValidSSNFormat_NullEmpty() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidSSNFormat(null));
    assertFalse(step.isValidSSNFormat(""));
    assertFalse(step.isValidSSNFormat("   "));
  }

  @Test
  void isValidSSNFormat_Valid() {
    ValidationFilterStep step = new ValidationFilterStep();
    assert(step.isValidSSNFormat("123-45-6780"));   // 9 digits with hyphen delimiters
    assert(step.isValidSSNFormat("123 45 6780"));   // 9 digits with space delimiters
    assert(step.isValidSSNFormat("123 45-6780"));   // 9 digits with mixed delimiters
    assert(step.isValidSSNFormat("123456780"));     // 9 digits with no delimiters
    assert(step.isValidSSNFormat("1234"));          // Has to be at least 4 characters
    assert(step.isValidSSNFormat("987456789"));     // Even though this is a TIN, we accept it
    assert(step.isValidSSNFormat(" - - 1234"));    // Allow multiple consecutive spaces/dashes

    // Make sure our rule to exclude 666-**-**** doesn't flag these invalid
    assert(step.isValidSSNFormat("6667"));
    assert(step.isValidSSNFormat("66678"));
    assert(step.isValidSSNFormat("666789"));

    // This is a specific check added 2020-10-20 in response to issues with SSN
    // validation to make sure that we don't want the last segment to be considered
    // invalid if it starts with two 9s.
    assert(step.isValidSSNFormat("9900"));

    // Trailing 0s are allowed, and make sure we account for 4-digits with leading space or hyphen
    assert(step.isValidSSNFormat("0001"));
    assert(step.isValidSSNFormat(" 0001"));
    assert(step.isValidSSNFormat("-0001"));
  }

  @Test
  void isValidSSNFormat_Invalid() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidSSNFormat("Pretty obviously not valid"));
    assertFalse(step.isValidSSNFormat("1-2-3"));
    assertFalse(step.isValidSSNFormat("666-45-6789"));  // Can't start with 666
    assertFalse(step.isValidSSNFormat("123"));          // Too short
    assertFalse(step.isValidSSNFormat("123A"));         // Right length, but non-numeric character
    assertFalse(step.isValidSSNFormat("1234567890"));   // Too long

    // http://www.dhs.state.il.us/page.aspx?item=14444
    assertFalse(step.isValidSSNFormat("123456789"));   // Sequential numbers
    assertFalse(step.isValidSSNFormat("444444444"));   // 9 identical digits
  }

  @Test
  void isValidSSNFormat_Invalid_ZeroSegments() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidSSNFormat("000-00-0000"));  // All 0s is a common invalid SSN in practice
    assertFalse(step.isValidSSNFormat("000-12-3456"));
    assertFalse(step.isValidSSNFormat("123-00-4567"));
    assertFalse(step.isValidSSNFormat("123-45-0000"));
    assertFalse(step.isValidSSNFormat("45-0000"));
    assertFalse(step.isValidSSNFormat("0000"));
    // Same checks, without delimiters
    assertFalse(step.isValidSSNFormat("000000000"));    // All 0s is a common invalid SSN in practice
    assertFalse(step.isValidSSNFormat("000123456"));
    assertFalse(step.isValidSSNFormat("123004567"));
    assertFalse(step.isValidSSNFormat("123450000"));
    assertFalse(step.isValidSSNFormat("450000"));
    assertFalse(step.isValidSSNFormat("0000"));
  }

  @Test
  void isValidSSNFormat_Valid_ZeroSegments() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertTrue(step.isValidSSNFormat("100-02-3456"));
    assertTrue(step.isValidSSNFormat("123-10-0567"));
    assertTrue(step.isValidSSNFormat("120-01-4567"));
    assertTrue(step.isValidSSNFormat("123-40-0001"));
    // Same checks, without delimiters
    assertTrue(step.isValidSSNFormat("100023456"));
    assertTrue(step.isValidSSNFormat("123100567"));
    assertTrue(step.isValidSSNFormat("120014567"));
    assertTrue(step.isValidSSNFormat("123400001"));
  }

  @Test
  void isValidSSNFormat_Invalid_NineSegments() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidSSNFormat("999-99-9999"));  // All 9s is a common invalid SSN in practice
    assertFalse(step.isValidSSNFormat("999-12-3456"));
    assertFalse(step.isValidSSNFormat("123-45-9999"));
    assertFalse(step.isValidSSNFormat("45-9999"));
    // Same checks, without delimiters
    assertFalse(step.isValidSSNFormat("999999999"));    // All 9s is a common invalid SSN in practice
    assertFalse(step.isValidSSNFormat("999123456"));
    assertFalse(step.isValidSSNFormat("123459999"));
    assertFalse(step.isValidSSNFormat("459999"));

    // Correction - discovered an issue on 2020-10-20 where we are incorrectly flagging SSNs invalid if the
    // middle segment is 99.  This is actually valid.  These tests are left in this same block, although they
    // are set up to confirm that we will allow 99 in the middle block.
    assertTrue(step.isValidSSNFormat("123-99-4567"));
    assertTrue(step.isValidSSNFormat("123994567"));
  }

  @Test
  void isValidSSNFormat_Valid_NineSegments() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertTrue(step.isValidSSNFormat("199-92-3456"));
    assertTrue(step.isValidSSNFormat("123-19-9567"));
    assertTrue(step.isValidSSNFormat("129-91-4567"));
    assertTrue(step.isValidSSNFormat("9-91-4567"));
    assertTrue(step.isValidSSNFormat("123-49-9991"));
    assertTrue(step.isValidSSNFormat("49-9991"));
    assertTrue(step.isValidSSNFormat("9-9991"));

    // Same checks, without delimiters
    assertTrue(step.isValidSSNFormat("199923456"));
    assertTrue(step.isValidSSNFormat("123199567"));
    assertTrue(step.isValidSSNFormat("129914567"));
    assertTrue(step.isValidSSNFormat("9914567"));
    assertTrue(step.isValidSSNFormat("123499991"));
    assertTrue(step.isValidSSNFormat("499991"));
    assertTrue(step.isValidSSNFormat("99991"));
  }

  @Test
  void isValidSSNFormat_Invalid_KnownSSNs() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidSSNFormat("078-05-1120"));
    assertFalse(step.isValidSSNFormat("078051120"));
    assertFalse(step.isValidSSNFormat("123-45-6789"));
    assertFalse(step.isValidSSNFormat("123456789"));
  }

  @Test
  void isValidPatientIdentifierFormat_NullEmpty() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidPatientIdentifierFormat(null));
    assertFalse(step.isValidPatientIdentifierFormat(""));
    assertFalse(step.isValidPatientIdentifierFormat("   "));
  }

  @Test
  void isValidPatientIdentifierFormat_Invalid() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidPatientIdentifierFormat("12345!"));    // Invalid character (!)
    assertFalse(step.isValidPatientIdentifierFormat("\t12345"));   // Tab (we only allow spaces)
    assertFalse(step.isValidPatientIdentifierFormat("_"));         // Requires at least one alpha-numeric
  }

  @Test
  void isValidPatientIdentifierFormat_Valid() {
    ValidationFilterStep step = new ValidationFilterStep();
    // Minimum of one alpha-numeric character
    assert(step.isValidPatientIdentifierFormat("0"));
    assert(step.isValidPatientIdentifierFormat("a"));
    assert(step.isValidPatientIdentifierFormat("A")); // Case sensitivity check

    // Verify all of the allowed special characters before
    assert(step.isValidPatientIdentifierFormat("00"));
    assert(step.isValidPatientIdentifierFormat("a0"));
    assert(step.isValidPatientIdentifierFormat("Aa0"));
    assert(step.isValidPatientIdentifierFormat(" 0"));
    assert(step.isValidPatientIdentifierFormat("-0"));
    assert(step.isValidPatientIdentifierFormat(".0"));
    assert(step.isValidPatientIdentifierFormat("#0"));
    assert(step.isValidPatientIdentifierFormat("_0"));

    // Verify all of the allowed special characters after
    assert(step.isValidPatientIdentifierFormat("00"));
    assert(step.isValidPatientIdentifierFormat("0a"));
    assert(step.isValidPatientIdentifierFormat("0aA"));
    assert(step.isValidPatientIdentifierFormat("0 "));
    assert(step.isValidPatientIdentifierFormat("0-"));
    assert(step.isValidPatientIdentifierFormat("0."));
    assert(step.isValidPatientIdentifierFormat("0#"));
    assert(step.isValidPatientIdentifierFormat("0_"));

    // More complete tests of mixed categories of characters
    assert(step.isValidPatientIdentifierFormat("100.100001a"));
    assert(step.isValidPatientIdentifierFormat("0 0 . 1"));
    assert(step.isValidPatientIdentifierFormat("      1"));
    assert(step.isValidPatientIdentifierFormat("asdf"));
    assert(step.isValidPatientIdentifierFormat("#10-60_93#a"));
    assert(step.isValidPatientIdentifierFormat("1a2B3c4D"));
  }

  @Test
  void isValidNameFormat_NullEmpty() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidNameFormat(null));
    assertFalse(step.isValidNameFormat(""));
    assertFalse(step.isValidNameFormat("   "));
  }

  @Test
  void isValidNameFormat_Invalid() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertFalse(step.isValidNameFormat("A"));    // <2 characters
    assertFalse(step.isValidNameFormat("A0"));   // <2 alphabetic characters
    assertFalse(step.isValidNameFormat("12345"));// No alphabetic characters
  }

  @Test
  void isValidNameFormat_Valid() {
    ValidationFilterStep step = new ValidationFilterStep();
    // Minimum of two alphabetic characters
    assert(step.isValidNameFormat("aa"));
    assert(step.isValidNameFormat("Aa"));    // Case-insensitive validation

    // Allow other non-alphabetic characters once we have two alphabetic characters
    assert(step.isValidNameFormat("A j"));   // Alpha characters don't have to be consecutive
    assert(step.isValidNameFormat("A.J."));  // Doesn't have to end with alpha
    assert(step.isValidNameFormat(" .J.A")); // Doesn't have to start with alpha
    assert(step.isValidNameFormat("  STEVEN  "));
    assert(step.isValidNameFormat("I'M GLAD THIS! ISN'T HOW MY #NAME IS spelled@#$%^&*()"));  // Anything goes!
  }
}
