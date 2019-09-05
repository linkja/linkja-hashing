package org.linkja.hashing.steps;

import org.apache.commons.lang.StringUtils;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;

public class ValidationFilterStep implements IStep {
  /**
   * Validation of date/time formats.  Note that because we are using the STRICT resolver, we need to use the Java 8
   * notation of year ('uuuu' instead of 'yyyy').
   */
  public static final DateTimeFormatter DATE_OF_BIRTH_FORMATTER = DateTimeFormatter.ofPattern(""
          + "[uuuu/M/d[ H:mm[:ss]]]"
          + "[uuuu-M-d[ H:mm[:ss]]]"
          + "[uuuu M d[ H:mm[:ss]]]"
          + "[uuuuMMdd[ H:mm[:ss]]]"
          + "[M/d/uuuu[ H:mm[:ss]]]"
          + "[M-d-uuuu[ H:mm[:ss]]]"
          + "[M d uuuu[ H:mm[:ss]]]"
          + "[MMdduuuu[ H:mm[:ss]]]"
  ).withResolverStyle(ResolverStyle.STRICT);

  // This is a specific SSN that is of appropriate length, but is considered an invalid placeholder and should be
  // removed
  public static final String INVALID_SSN = "0000";

  /**
   * Regex to determine if SSN is valid
   * Adapted from 4.12. Validate Social Security Numbers, in "Regular Expressions Cookbook, 2nd Edition" with the
   * following changes:
   *  - Allow 900-999 (TINs)
   *  - Allow hyphen or space delimiters
   */
  //public Pattern DelimitedSSNRegex = Pattern.compile("^(?!000)[0-9]{0,3}[- ]?(?!00)[0-9]{0,2}[- ]?(?!0000)[0-9]{4}$");
  public Pattern SSNRegex = Pattern.compile("^(?!000)[0-9]{0,3}[- ]?(?!00)[0-9]{0,2}[- ]?(?!0000)[0-9]{4}$");

  /**
   * Secondary regex to check if SSN is valid - assumes delimiters have been stripped.
   */
  //public Pattern SSNRegex = Pattern.compile("^[0-9]{4,9}$");

  public static final int MIN_SSN_LENGTH = 4;

  /**
   * SSNs that are considered invalid because they have been used in advertisements.
   * These must be formatted as 9 digits with no delimiters
   */
  public static final String[] BLACKLISTED_SSNS = new String[] {
    "078051120"
  };

  @Override
  public DataRow run(DataRow row) {
    if (row == null || !row.shouldProcess()) {
      return row;
    }
    row.addCompletedStep(this.getStepName());

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
    if (row.containsKey(Engine.FIRST_NAME_FIELD) && row.get(Engine.FIRST_NAME_FIELD).trim().length() < Engine.MIN_NAME_LENGTH) {
      lengthBuilder.append("First Name, ");
      hasLengthError = true;
    }
    if (row.containsKey(Engine.LAST_NAME_FIELD) && row.get(Engine.LAST_NAME_FIELD).trim().length() < Engine.MIN_NAME_LENGTH) {
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
    if (row.containsKey(Engine.SOCIAL_SECURITY_NUMBER) && !isValidSSN(row.get(Engine.SOCIAL_SECURITY_NUMBER))) {
      formatBuilder.append("Social Security Number, ");
      hasFormatError = true;
    }

    if (hasFormatError) {
      row.setInvalidReason(safeAppendInvalidReason(row, formatBuilder.toString()));
    }

    return row;
  }

  /**
   * Because the SSN field is optional, this check will ensure that we are only doing a validation check when it is
   * appropriate.  Note that if we shouldn't validate the SSN, it should be explicitly blanked out later on.  This is
   * not the responsibility of the validation step to do, however.
   * @param ssnString
   * @return
   */
  public boolean shouldValidateSSN(String ssnString) {
    return (ssnString != null) && (!ssnString.trim().equals(""));
  }

  /**
   * Determine if a string representing an SSN is valid or not.  Note that we are lumping TINs in with SSNs, although
   * they are slightly different.
   * @param ssnString
   * @return
   */
  public boolean isValidSSN(String ssnString) {
    if (!shouldValidateSSN(ssnString)) {
      return true;
    }

    String strippedSSN = ssnString.trim().replaceAll("[\\- ]", "");
    if (strippedSSN.length() < MIN_SSN_LENGTH) {
      return false;
    }

    // If it's beyond 9 characters, something is wrong - either too many digits or invalid delimiters.  Regardless,
    // we're done checking at this point.
    if (strippedSSN.length() > 9) {
      return false;
    }
    // If it's 9 digits long, we need to check against our blacklist
    else if (strippedSSN.length() == 9) {
      boolean blacklisted = Arrays.stream(BLACKLISTED_SSNS).anyMatch(x -> x.equals(strippedSSN));
      if (blacklisted) {
        return false;
      }

      // One edge condition not covered in our regex is starting with 666-**-****
      if (strippedSSN.startsWith("666")) {
        return false;
      }
    }

//    if (!DelimitedSSNRegex.matcher(ssnString).matches()) {
//      return false;
//    }

    // At this point, it should be 4-9 digits
    return SSNRegex.matcher(strippedSSN).matches();
  }

  /**
   * Determine if a string, which should represent a date, is in one of our allowed input date formats.
   * @param dateString The string to parse
   * @return true if the string appears to be a date, false otherwise.
   */
  public boolean isValidDate(String dateString) {
    if (dateString == null) {
      return false;
    }

    try {
      LocalDate.parse(dateString.trim(), DATE_OF_BIRTH_FORMATTER);
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
