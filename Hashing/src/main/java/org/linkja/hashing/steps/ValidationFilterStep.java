package org.linkja.hashing.steps;

import org.apache.commons.lang.StringUtils;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

public class ValidationFilterStep implements IStep {
  @Override
  public DataRow run(DataRow row) {
    checkBlankFields(row);
    return row;
  }

  /**
   * Determine if required fields are present in the data row.  We consider whitespace fields as blank during
   * this check.
   * @param row The DataRow to check
   * @return DataRow with the invalid reason set (if any problems were found)
   */
  public DataRow checkBlankFields(DataRow row) {
    if (row == null) {
      return null;
    }

    if (!row.shouldProcess()) {
      return row;
    }

    // We require that the 4 primary fields have non-empty values.  Note that instead of short-circuiting on the first
    // failure, we are performing a comprehensive check to identify all issues.
    boolean hasError = false;
    StringBuilder builder = new StringBuilder();
    builder.append("The following fields are missing or just contain whitespace.  They must be filled in: ");
    if (!row.containsKey(Engine.PATIENT_ID_FIELD) || row.get(Engine.PATIENT_ID_FIELD).trim().isEmpty()) {
      builder.append("Patient Identifier, ");
      hasError = true;
    }
    if (!row.containsKey(Engine.FIRST_NAME_FIELD) || row.get(Engine.FIRST_NAME_FIELD).trim().isEmpty()) {
      builder.append("First Name, ");
      hasError = true;
    }
    if (!row.containsKey(Engine.LAST_NAME_FIELD) || row.get(Engine.LAST_NAME_FIELD).trim().isEmpty()) {
      builder.append("Last Name, ");
      hasError = true;
    }
    if (!row.containsKey(Engine.DATE_OF_BIRTH_FIELD) || row.get(Engine.DATE_OF_BIRTH_FIELD).trim().isEmpty()) {
      builder.append("Date of Birth, ");
      hasError = true;
    }

    if (hasError) {
      row.setInvalidReason(StringUtils.strip(builder.toString(), ", ").trim());
    }

    return row;
  }
}
