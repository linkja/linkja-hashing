package org.linkja.hashing.steps;

import org.junit.jupiter.api.Test;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class PermuteStepTest {
  private static final String[] DELIMITERS = new String[] { " ", "-" };

  @Test
  void permuteLastName_NullEmpty() {
    PermuteStep step = new PermuteStep();
    assertNull(step.permuteLastName(null));

    DataRow emptyRow = step.permuteLastName(new DataRow());
    assertNull(emptyRow.get(Engine.LAST_NAME_FIELD));
    assertNull(emptyRow.getDerivedRows());

    emptyRow.put(Engine.LAST_NAME_FIELD, "");
    assertEquals("", emptyRow.get(Engine.LAST_NAME_FIELD));
    assertNull(emptyRow.getDerivedRows());


    emptyRow.put(Engine.LAST_NAME_FIELD, "       ");
    emptyRow = step.permuteLastName(emptyRow);
    assertEquals("       ", emptyRow.get(Engine.LAST_NAME_FIELD));
    assertNull(emptyRow.getDerivedRows());
  }

  @Test
  void permuteLastName_SingleName() {
    PermuteStep step = new PermuteStep();
    DataRow emptyRow = step.permuteLastName(new DataRow());
    emptyRow.put(Engine.LAST_NAME_FIELD, "SMITH");
    assertEquals("SMITH", emptyRow.get(Engine.LAST_NAME_FIELD));
    assertNull(emptyRow.getDerivedRows());
  }

  @Test
  void permuteLastName_TwoNames() {
    PermuteStep step = new PermuteStep();
    String expectedOriginalLastName = "SMITHOLSON";
    for (String delimiter : DELIMITERS) {
      DataRow row = new DataRow();
      row.put(Engine.LAST_NAME_FIELD, String.format("SMITH%sOLSON", delimiter));
      row = step.permuteLastName(row);
      assertEquals(expectedOriginalLastName, row.get(Engine.LAST_NAME_FIELD));
      ArrayList<DataRow> derivedRows = row.getDerivedRows();
      assertEquals(2, derivedRows.size());
      assertEquals("SMITH", derivedRows.get(0).get(Engine.LAST_NAME_FIELD));
      assertEquals("OLSON", derivedRows.get(1).get(Engine.LAST_NAME_FIELD));
    }
  }

  @Test
  void permuteLastName_TwoNamesTooShort() {
    PermuteStep step = new PermuteStep();
    String expectedOriginalLastName = "DC";
    for (String delimiter : DELIMITERS) {
      DataRow row = new DataRow();
      row.put(Engine.LAST_NAME_FIELD, String.format("D%sC", delimiter));
      row = step.permuteLastName(row);
      assertEquals(expectedOriginalLastName, row.get(Engine.LAST_NAME_FIELD));

      // Because the individual components of the name are too short (1 character each), they shouldn't be included
      // as derived rows.
      assertFalse(row.hasDerivedRows());
    }
  }

  @Test
  void permuteLastName_TwoNamesFirstTooShort() {
    PermuteStep step = new PermuteStep();
    String expectedOriginalLastName = "FSECOND";
    for (String delimiter : DELIMITERS) {
      DataRow row = new DataRow();
      row.put(Engine.LAST_NAME_FIELD, String.format("F%sSECOND", delimiter));
      row = step.permuteLastName(row);
      assertEquals(expectedOriginalLastName, row.get(Engine.LAST_NAME_FIELD));
      ArrayList<DataRow> derivedRows = row.getDerivedRows();
      assertEquals(1, derivedRows.size());
      assertEquals("SECOND", derivedRows.get(0).get(Engine.LAST_NAME_FIELD));
    }
  }

  @Test
  void permuteLastName_TwoNamesSecondTooShort() {
    PermuteStep step = new PermuteStep();
    String expectedOriginalLastName = "FIRSTS";
    for (String delimiter : DELIMITERS) {
      DataRow row = new DataRow();
      row.put(Engine.LAST_NAME_FIELD, String.format("FIRST%sS", delimiter));
      row = step.permuteLastName(row);
      assertEquals(expectedOriginalLastName, row.get(Engine.LAST_NAME_FIELD));
      ArrayList<DataRow> derivedRows = row.getDerivedRows();
      assertEquals(1, derivedRows.size());
      assertEquals("FIRST", derivedRows.get(0).get(Engine.LAST_NAME_FIELD));
    }
  }

  @Test
  void permuteLastName_SeveralNames() {
    PermuteStep step = new PermuteStep();
    for (String delimiter : DELIMITERS) {
      DataRow row = new DataRow();
      row.put(Engine.LAST_NAME_FIELD, String.format("SMITH%sOLSON%sJONES%sDOE", delimiter, delimiter, delimiter));
      row = step.permuteLastName(row);
      assertEquals("SMITHOLSONJONESDOE", row.get(Engine.LAST_NAME_FIELD));
      ArrayList<DataRow> derivedRows = row.getDerivedRows();
      assertEquals(2, derivedRows.size());
      assertEquals("SMITH", derivedRows.get(0).get(Engine.LAST_NAME_FIELD));
      assertEquals("DOE", derivedRows.get(1).get(Engine.LAST_NAME_FIELD));
    }
  }

  @Test
  void run_TracksCompletedStep() {
    DataRow row = new DataRow() {{
      put(Engine.FIRST_NAME_FIELD, "JOE ");
      put(Engine.PATIENT_ID_FIELD, "123456");
      put(Engine.LAST_NAME_FIELD, "SMITH");
    }};

    PermuteStep step = new PermuteStep();
    row = step.run(row);
    assert(row.hasCompletedStep(step.getStepName()));
  }

  @Test
  void run_DoesNotTrackCompletedStepWhenInvalid() {
    DataRow row = new DataRow() {{
      put(Engine.FIRST_NAME_FIELD, "JOE ");
      put(Engine.PATIENT_ID_FIELD, "123456");
      put(Engine.LAST_NAME_FIELD, "SMITH");
    }};

    row.setInvalidReason("Invalid for testing purposes");
    PermuteStep step = new PermuteStep();
    row = step.run(row);
    assertFalse(row.hasCompletedStep(step.getStepName()));
  }

  @Test
  void run_StripsInvalidCharsFromName() {
    DataRow row = new DataRow() {{
      put(Engine.FIRST_NAME_FIELD, " JO ELLEN ");
      put(Engine.PATIENT_ID_FIELD, "123456");
      put(Engine.LAST_NAME_FIELD, " SMITH ");
    }};

    PermuteStep step = new PermuteStep();
    row = step.run(row);
    assertEquals("JOELLEN", row.get(Engine.FIRST_NAME_FIELD));
    assertEquals("SMITH", row.get(Engine.LAST_NAME_FIELD));
  }
}