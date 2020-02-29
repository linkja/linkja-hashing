package org.linkja.hashing;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

/**
 * A representation of patient data that is processed by the system.
 *
 * Intended to be flexible to contain any data elements, the key of the collection will represent the canonical field
 * identifier.  The value will be the value to used (as it may be modified by a processing step).  There are additional
 * status fields that are used to describe the state of this data row.
 */
public class DataRow extends HashMap<String, Object> implements Cloneable {
  private long rowNumber;
  private String invalidReason;
  private String warning;
  private boolean isException;
  private ArrayList<DataRow> derivedRows;
  private ArrayList<String> completedSteps;

  public long getRowNumber() { return rowNumber; }

  public void setRowNumber(long rowNumber) { this.rowNumber = rowNumber; }

  public String getInvalidReason() {
    return invalidReason;
  }

  public void setInvalidReason(String invalidReason) {
    this.invalidReason = invalidReason;
  }

  public String getWarning() {
    return warning;
  }

  public void setWarning(String warning) {
    this.warning = warning;
  }

  public boolean isException() {
    return isException;
  }

  public void setException(boolean exception) {
    isException = exception;
  }

  public ArrayList<DataRow> getDerivedRows() {
    return derivedRows;
  }

  public void setDerivedRows(ArrayList<DataRow> derivedRows) {
    this.derivedRows = derivedRows;
  }

  public ArrayList<String> getCompletedSteps() {
    return completedSteps;
  }

  public void setCompletedSteps(ArrayList<String> completedSteps) {
    this.completedSteps = completedSteps;
  }

  public void addCompletedStep(String step) {
    if (this.completedSteps == null) {
      this.completedSteps = new ArrayList<String>();
    }
    this.completedSteps.add(step);
  }

  public boolean hasCompletedStep(String step) {
    if (this.completedSteps == null) {
      return false;
    }

    return this.completedSteps.contains(step);
  }

  /**
   * Adds a DataRow to the derived rows collection for this particular row
   * @param row
   */
  public void addDerivedRow(DataRow row) {
    if (row == null) {
      return;
    }

    if (this.derivedRows == null) {
      this.derivedRows = new ArrayList<DataRow>();
    }

    this.derivedRows.add(row);
  }

  /**
   * Updates an existing data row in our list of derived rows.
   * @param oldRow
   * @param newRow
   * @throws Exception
   */
  public void updateDerivedRow(DataRow oldRow, DataRow newRow) throws Exception {
    // Find the old row.  This assumes the old row exists, so we do not fail gracefully if it's not found
    int oldRowIndex = this.derivedRows.indexOf(oldRow);
    if (oldRowIndex < 0) {
      throw new Exception("Attempted to update a derived row, but the existing object was not found");
    }

    // If the user tells us to update the derived row with a null one, we will delete the entry entirely to clean up
    // the collection.
    if (newRow == null) {
      this.derivedRows.remove(oldRowIndex);
    }
    else {
      this.derivedRows.set(oldRowIndex, newRow);
    }
  }

  /**
   * Determine if this data row still qualifies for additional processing steps.  This can be changed by actions in
   * different processing steps.
   * @return
   */
  public boolean shouldProcess() {
    return (this.invalidReason == null || this.invalidReason.isEmpty());
  }

  /**
   * Quick function to determine if there are any derived rows for this data row.
   * @return
   */
  public boolean hasDerivedRows() {
    return (this.derivedRows != null) && this.derivedRows.size() > 0;
  }

  @Override
  public Object clone() {
    DataRow cloneRow = (DataRow)super.clone();
    cloneRow.setRowNumber(this.getRowNumber());
    cloneRow.setInvalidReason(this.getInvalidReason());
    cloneRow.setWarning(this.getWarning());
    cloneRow.setException(this.isException());

    if (this.getDerivedRows() == null) {
      cloneRow.setDerivedRows(null);
    }
    else {
      ArrayList<DataRow> cloneDerivedRows = new ArrayList<DataRow>();
      for (DataRow derivedRow : this.derivedRows) {
        cloneDerivedRows.add((DataRow)derivedRow.clone());
      }
      cloneRow.setDerivedRows(cloneDerivedRows);
    }

    if (this.getCompletedSteps() == null) {
      cloneRow.setCompletedSteps(null);
    }
    else {
      ArrayList<String> cloneCompletedSteps = new ArrayList<String>();
      for (String step : this.completedSteps) {
        cloneCompletedSteps.add(step);
      }
      cloneRow.setCompletedSteps(cloneCompletedSteps);
    }

    return cloneRow;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (obj == this) {
      return true;
    }

    if (!DataRow.class.isAssignableFrom(obj.getClass())) {
      return false;
    }

    final DataRow otherRow = (DataRow)obj;
    if (!super.equals((HashMap<String, Object>)otherRow)) {
      return false;
    }

    if (Objects.equals(this.getRowNumber(), otherRow.getRowNumber())
            && Objects.equals(this.isException(), otherRow.isException())
            && Objects.equals(this.getInvalidReason(), otherRow.getInvalidReason())
            && Objects.equals(this.getWarning(), otherRow.getWarning())
            && Objects.equals(this.getDerivedRows(), otherRow.getDerivedRows())
            && Objects.equals(this.getCompletedSteps(), otherRow.getCompletedSteps())) {
      return true;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(19, 73)
            .append(this.rowNumber)
            .append(this.isException)
            .append(this.invalidReason)
            .append(this.warning)
            .append(this.derivedRows)
            .append(this.completedSteps)
            .toHashCode();
  }
}
