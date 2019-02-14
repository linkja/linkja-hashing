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
    for (String delimiter : DELIMITERS) {
      DataRow row = new DataRow();
      row.put(Engine.LAST_NAME_FIELD, String.format("SMITH%sOLSON", delimiter));
      row = step.permuteLastName(row);
      assertEquals(String.format("SMITH%sOLSON", delimiter), row.get(Engine.LAST_NAME_FIELD));
      ArrayList<DataRow> derivedRows = row.getDerivedRows();
      assertEquals(2, derivedRows.size());
      assertEquals("SMITH", derivedRows.get(0).get(Engine.LAST_NAME_FIELD));
      assertEquals("OLSON", derivedRows.get(1).get(Engine.LAST_NAME_FIELD));
    }
  }

  @Test
  void permuteLastName_SeveralNames() {
    PermuteStep step = new PermuteStep();
    for (String delimiter : DELIMITERS) {
      DataRow row = new DataRow();
      row.put(Engine.LAST_NAME_FIELD, String.format("SMITH%sOLSON%sJONES%sDOE", delimiter, delimiter, delimiter));
      row = step.permuteLastName(row);
      assertEquals(String.format("SMITH%sOLSON%sJONES%sDOE", delimiter, delimiter, delimiter), row.get(Engine.LAST_NAME_FIELD));
      ArrayList<DataRow> derivedRows = row.getDerivedRows();
      assertEquals(2, derivedRows.size());
      assertEquals("SMITH", derivedRows.get(0).get(Engine.LAST_NAME_FIELD));
      assertEquals("DOE", derivedRows.get(1).get(Engine.LAST_NAME_FIELD));
    }
  }
}