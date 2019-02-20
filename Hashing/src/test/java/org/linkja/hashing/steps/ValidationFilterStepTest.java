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
    row.put(Engine.DATE_OF_BIRTH_FIELD, "12/12/1912");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "333-3E-3333");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.checkFieldFormat(row);
    assertEquals("The following fields are not in a valid format: Social Security Number (only allow numbers, dashes and spaces)",
            row.getInvalidReason());

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
  void checkFieldFormat_ValidFormat() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "12/12/1912");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "333-33-3333");

    ValidationFilterStep step = new ValidationFilterStep();
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
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Patient Identifier, Date of Birth\r\nThe following fields must be longer than 1 character: First Name, Last Name\r\nThe following fields are not in a valid format: Social Security Number (only allow numbers, dashes and spaces)",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.FIRST_NAME_FIELD, "JON");
    row.put(Engine.LAST_NAME_FIELD, "DOE");
    row = step.run(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Patient Identifier, Date of Birth\r\nThe following fields are not in a valid format: Social Security Number (only allow numbers, dashes and spaces)",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.PATIENT_ID_FIELD, "12345");
    row.put(Engine.FIRST_NAME_FIELD, "A");
    row.put(Engine.LAST_NAME_FIELD, "B");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "12/12/1912");
    row = step.run(row);
    assertEquals("The following fields must be longer than 1 character: First Name, Last Name\r\nThe following fields are not in a valid format: Social Security Number (only allow numbers, dashes and spaces)",
            row.getInvalidReason());

    row.setInvalidReason(null);
    row.put(Engine.PATIENT_ID_FIELD, "");
    row.put(Engine.FIRST_NAME_FIELD, "A");
    row.put(Engine.LAST_NAME_FIELD, "B");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "");
    row.put(Engine.SOCIAL_SECURITY_NUMBER, "3333");
    row = step.run(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Patient Identifier, Date of Birth\r\nThe following fields must be longer than 1 character: First Name, Last Name",
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

    row.put(Engine.SOCIAL_SECURITY_NUMBER, "333-33 3333");
    row = step.run(row);
    assertNull(row.getInvalidReason());
  }
}