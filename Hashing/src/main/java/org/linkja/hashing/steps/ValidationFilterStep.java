package org.linkja.hashing.steps;

import org.apache.commons.lang.StringUtils;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class ValidationFilterStep implements IStep {
  public static final int MIN_NAME_LENGTH = 2;
  private static final String SSN_INVALID_CHARACTERS = ".*[^\\d\\- ].*";
  public static final DateTimeFormatter DATE_OF_BIRTH_FORMATTER = DateTimeFormatter.ofPattern(""
          + "[yyyy/M/d[ H:mm[:ss]]]"
          + "[yyyy-M-d[ H:mm[:ss]]]"
          + "[yyyy M d[ H:mm[:ss]]]"
          + "[yyyyMMdd[ H:mm[:ss]]]"
          + "[M/d/yyyy[ H:mm[:ss]]]"
          + "[M-d-yyyy[ H:mm[:ss]]]"
          + "[M d yyyy[ H:mm[:ss]]]"
          + "[MMddyyyy[ H:mm[:ss]]]"
  );

  @Override
  public DataRow run(DataRow row) {
    if (row == null || !row.shouldProcess()) {
      return row;
    }

    row = checkRequiredFields(row);
    row = checkFieldLength(row);
    row = checkFieldFormat(row);
    return row;
  }

  @Override
  public String getStepName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Determine if required fields are present in the data row.  We consider whitespace fields as blank during
   * this check.
   * @param row The DataRow to check
   * @return DataRow with the invalid reason set (if any problems were found)
   */
  public DataRow checkRequiredFields(DataRow row) {
    if (row == null) {
      return null;
    }

    // We require that the 4 primary fields have non-empty values.  Note that instead of short-circuiting on the first
    // failure, we are performing a comprehensive check to identify all issues.
    boolean hasMissingError = false;
    StringBuilder missingBuilder = new StringBuilder();
    missingBuilder.append("The following fields are missing or just contain whitespace.  They must be filled in: ");
    if (!row.containsKey(Engine.PATIENT_ID_FIELD) || row.get(Engine.PATIENT_ID_FIELD).trim().isEmpty()) {
      missingBuilder.append("Patient Identifier, ");
      hasMissingError = true;
    }
    if (!row.containsKey(Engine.FIRST_NAME_FIELD) || row.get(Engine.FIRST_NAME_FIELD).trim().isEmpty()) {
      missingBuilder.append("First Name, ");
      hasMissingError = true;
    }
    if (!row.containsKey(Engine.LAST_NAME_FIELD) || row.get(Engine.LAST_NAME_FIELD).trim().isEmpty()) {
      missingBuilder.append("Last Name, ");
      hasMissingError = true;
    }
    if (!row.containsKey(Engine.DATE_OF_BIRTH_FIELD) || row.get(Engine.DATE_OF_BIRTH_FIELD).trim().isEmpty()) {
      missingBuilder.append("Date of Birth, ");
      hasMissingError = true;
    }

    if (hasMissingError) {
      row.setInvalidReason(safeAppendInvalidReason(row, missingBuilder.toString()));
    }

    return row;
  }

  /**
   * Validate that data fields with length requirements meet those requirements
   * @param row The DataRow to check
   * @return DataRow with the invalid reason set (if any problems were found)
   */
  public DataRow checkFieldLength(DataRow row) {
    if (row == null) {
      return null;
    }

    boolean hasLengthError = false;
    StringBuilder lengthBuilder = new StringBuilder();
    lengthBuilder.append("The following fields must be longer than 1 character: ");
    if (row.containsKey(Engine.FIRST_NAME_FIELD) && row.get(Engine.FIRST_NAME_FIELD).trim().length() < MIN_NAME_LENGTH) {
      lengthBuilder.append("First Name, ");
      hasLengthError = true;
    }
    if (row.containsKey(Engine.LAST_NAME_FIELD) && row.get(Engine.LAST_NAME_FIELD).trim().length() < MIN_NAME_LENGTH) {
      lengthBuilder.append("Last Name, ");
      hasLengthError = true;
    }

    if (hasLengthError) {
      row.setInvalidReason(safeAppendInvalidReason(row, lengthBuilder.toString()));
    }

    return row;
  }

  /**
   * Validate the format of fields which have formatting requirements (DOB, SSN)
   * @param row The DataRow to check
   * @return DataRow with the invalid reason set (if any problems were found)
   */
  public DataRow checkFieldFormat(DataRow row) {
    if (row == null) {
      return null;
    }

    boolean hasFormatError = false;
    StringBuilder formatBuilder = new StringBuilder();
    formatBuilder.append("The following fields are not in a valid format: ");
    if (row.containsKey(Engine.DATE_OF_BIRTH_FIELD) && !isValidDate(row.get(Engine.DATE_OF_BIRTH_FIELD))) {
      formatBuilder.append("Date of Birth (recommended to use MM/DD/YYYY format), ");
      hasFormatError = true;
    }
    if (row.containsKey(Engine.SOCIAL_SECURITY_NUMBER) && row.get(Engine.SOCIAL_SECURITY_NUMBER).trim().matches(SSN_INVALID_CHARACTERS)) {
      formatBuilder.append("Social Security Number (only allow numbers, dashes and spaces), ");
      hasFormatError = true;
    }

    if (hasFormatError) {
      row.setInvalidReason(safeAppendInvalidReason(row, formatBuilder.toString()));
    }

    return row;
  }

  public boolean isValidDate(String dateString) {
    if (dateString == null) {
      return false;
    }

    try {
      LocalDate.parse(dateString, DATE_OF_BIRTH_FORMATTER);
      return true;
    }
    catch (DateTimeParseException exc) {
      return false;
    }
  }

  /**
   * Utility method to safely append a reason why a data row is invalid, accounting for nulls, trimming spaces, etc.
   * @param row The DataRow to add the invalid reason to
   * @param additionalReason The description why the row is invalid, which needs to be appended
   * @return The updated invalid reason, with additionalReason appended
   */
  private String safeAppendInvalidReason(DataRow row, String additionalReason) {
    String reason = (Optional.ofNullable(row.getInvalidReason()).orElse("") + "\r\n" +
            StringUtils.strip(additionalReason, ", ").trim()).trim();
    return reason;
  }
}
