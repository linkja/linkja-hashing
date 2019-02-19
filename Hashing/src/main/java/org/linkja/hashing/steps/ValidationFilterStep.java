package org.linkja.hashing.steps;

import org.apache.commons.lang.StringUtils;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

import java.util.Optional;

public class ValidationFilterStep implements IStep {
  public static final int MIN_NAME_LENGTH = 2;

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
    boolean hasMissingError = false;
    StringBuilder missingBuilder = new StringBuilder();
    missingBuilder.append("The following fields are missing or just contain whitespace.  They must be filled in: ");
    boolean hasLengthError = false;
    StringBuilder lengthBuilder = new StringBuilder();
    lengthBuilder.append("The following fields must be longer than 1 character: ");
    if (!row.containsKey(Engine.PATIENT_ID_FIELD) || row.get(Engine.PATIENT_ID_FIELD).trim().isEmpty()) {
      missingBuilder.append("Patient Identifier, ");
      hasMissingError = true;
    }
    if (!row.containsKey(Engine.FIRST_NAME_FIELD) || row.get(Engine.FIRST_NAME_FIELD).trim().isEmpty()) {
      missingBuilder.append("First Name, ");
      hasMissingError = true;
    }
    else if (row.get(Engine.FIRST_NAME_FIELD).trim().length() < MIN_NAME_LENGTH) {
      lengthBuilder.append("First Name, ");
      hasLengthError = true;
    }
    if (!row.containsKey(Engine.LAST_NAME_FIELD) || row.get(Engine.LAST_NAME_FIELD).trim().isEmpty()) {
      missingBuilder.append("Last Name, ");
      hasMissingError = true;
    }
    else if (row.get(Engine.LAST_NAME_FIELD).trim().length() < MIN_NAME_LENGTH) {
      lengthBuilder.append("Last Name, ");
      hasLengthError = true;
    }
    if (!row.containsKey(Engine.DATE_OF_BIRTH_FIELD) || row.get(Engine.DATE_OF_BIRTH_FIELD).trim().isEmpty()) {
      missingBuilder.append("Date of Birth, ");
      hasMissingError = true;
    }

    if (hasMissingError) {
      row.setInvalidReason(StringUtils.strip(missingBuilder.toString(), ", ").trim());
    }
    if (hasLengthError) {
      String reason = (Optional.ofNullable(row.getInvalidReason()).orElse("") + "\r\n" +
              StringUtils.strip(lengthBuilder.toString(), ", ").trim()).trim();
      row.setInvalidReason(reason);
    }

    return row;
  }
}
