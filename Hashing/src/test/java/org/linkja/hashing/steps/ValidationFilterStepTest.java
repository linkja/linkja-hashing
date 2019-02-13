package org.linkja.hashing.steps;

import org.junit.jupiter.api.Test;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;
import org.linkja.hashing.steps.ValidationFilterStep;

import static org.junit.jupiter.api.Assertions.*;

class ValidationFilterStepTest {

  @Test
  void checkBlankFields_Null() {
    ValidationFilterStep step = new ValidationFilterStep();
    assertNull(step.checkBlankFields(null));
  }

  @Test
  void checkBlankFields_Empty() {
    DataRow row = new DataRow();
    ValidationFilterStep step = new ValidationFilterStep();
    assertEquals(row, step.checkBlankFields(row));
  }

  @Test
  void checkBlankFields_Missing() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "");
    row.put(Engine.FIRST_NAME_FIELD, "  ");
    row.put(Engine.LAST_NAME_FIELD, "\t");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.checkBlankFields(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Patient Identifier, First Name, Last Name, Date of Birth",
            row.getInvalidReason());

    row.put(Engine.PATIENT_ID_FIELD, "1");
    row = step.checkBlankFields(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: First Name, Last Name, Date of Birth",
            row.getInvalidReason());

    row.put(Engine.LAST_NAME_FIELD, "Test");
    row = step.checkBlankFields(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: First Name, Date of Birth",
            row.getInvalidReason());

    row.put(Engine.FIRST_NAME_FIELD, "Patient");
    row = step.checkBlankFields(row);
    assertEquals("The following fields are missing or just contain whitespace.  They must be filled in: Date of Birth",
            row.getInvalidReason());
  }

  @Test
  void checkBlankFields_SkipsChecksIfFlaggedNotToProcess() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "");
    row.put(Engine.FIRST_NAME_FIELD, "  ");
    row.put(Engine.LAST_NAME_FIELD, "\t");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "");
    row.setInvalidReason("Already invalid");

    ValidationFilterStep step = new ValidationFilterStep();

    // We're making sure that we don't perform the checks (which would update the invalid reason for this data row) because
    // the row is already marked as invalid.
    row = step.checkBlankFields(row);
    assertEquals("Already invalid", row.getInvalidReason());
  }

  @Test
  void checkBlankFields_Filled() {
    DataRow row = new DataRow();
    row.put(Engine.PATIENT_ID_FIELD, "1");
    row.put(Engine.FIRST_NAME_FIELD, "Patient");
    row.put(Engine.LAST_NAME_FIELD, "Test");
    row.put(Engine.DATE_OF_BIRTH_FIELD, "05/15/2000");

    ValidationFilterStep step = new ValidationFilterStep();
    row = step.checkBlankFields(row);
    assertEquals(null, row.getInvalidReason());
  }
}