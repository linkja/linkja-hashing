package org.linkja.hashing;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class DataRowTest {

  @Test
  void clone_Empty() {
    DataRow row = new DataRow();
    DataRow cloneRow = (DataRow)row.clone();
    assert(row != cloneRow);      // Verify not the same object
    assertEquals(row, cloneRow);  // Verify has the same content
    assertEquals(row.hashCode(), cloneRow.hashCode());
  }

  @Test
  void clone_Deep() {
    DataRow row = new DataRow();
    row.put("one", "two");
    row.put("three", "four");
    row.setRowNumber(1000);
    row.setException(false);
    row.setInvalidReason("Some invalid reason");
    row.setWarning("Some warning");
    ArrayList<DataRow> derivedRows = new ArrayList<DataRow>();
    DataRow derivedRow = new DataRow();
    derivedRow.put("five", "six");
    derivedRows.add(derivedRow);
    row.setDerivedRows(derivedRows);

    DataRow cloneRow = (DataRow)row.clone();

    assertEquals(row.getInvalidReason(), cloneRow.getInvalidReason());
    assertEquals(row.getRowNumber(), cloneRow.getRowNumber());
    assertEquals(row.isException(), cloneRow.isException());
    assertEquals(row.getWarning(), cloneRow.getWarning());
    assertEquals(row.getDerivedRows(), cloneRow.getDerivedRows());
    assertEquals(row, cloneRow);
    assertEquals(row.hashCode(), cloneRow.hashCode());

    // Now we change the original and confirm only that has changed
    row.put("seven", "eight");
    row.setRowNumber(1001);
    row.setException(true);
    row.setInvalidReason("");
    row.setWarning("");
    derivedRow.setInvalidReason("Derived row invalid reason");

    assertNotEquals(row.getRowNumber(), cloneRow.getRowNumber());
    assertNotEquals(row.getInvalidReason(), cloneRow.getInvalidReason());
    assertNotEquals(row.isException(), cloneRow.isException());
    assertNotEquals(row.getWarning(), cloneRow.getWarning());
    assertNotEquals(row.getDerivedRows(), cloneRow.getDerivedRows());
    assertNotEquals(row, cloneRow);
    assertNotEquals(row.hashCode(), cloneRow.hashCode());
  }

  @Test
  void shouldProcess_Default() {
    DataRow row = new DataRow();
    assert(row.shouldProcess());
  }

  @Test
  void shouldProcess_ExceptionFlag() {
    DataRow row = new DataRow();
    row.setException(true);
    assert(row.shouldProcess());
  }

  @Test
  void shouldProcess_Warning() {
    DataRow row = new DataRow();
    row.setWarning("Just FYI");
    assert(row.shouldProcess());
  }

  @Test
  void shouldProcess_InvalidReason() {
    DataRow row = new DataRow();
    row.setInvalidReason("Something is wrong with this data");
    assertFalse(row.shouldProcess());
    row.setInvalidReason("");
    assert(row.shouldProcess());
  }
}