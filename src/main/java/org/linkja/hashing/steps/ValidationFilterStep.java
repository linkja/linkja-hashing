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
   *  - Allow hyphen or space delimiters (can be optional)
   *  - Exclude check for first 3 digits being 666 (check this separately)
   *  - Matches ranges of 4-9 numbers with optional delimiters
   *
   * Note that we are removing hyphen and space characters from the regex to simplify it. The first step in our
   * process is to strip those characters, and so they aren't needed.
   *
   * We do allow regex of varying lengths.  We are assuming that any length <9 digits means we are getting the
   * trailing SSN digits.  There is no way for us to know that for sure.
   */
  public static final Pattern SSN9Regex = Pattern.compile("^(?!000|999)[0-9]{3}(?!00)[0-9]{2}(?!0000|9999)[0-9]{4}$");
  public static final Pattern SSN8Regex = Pattern.compile("^[0-9]{2}(?!00)[0-9]{2}(?!0000|9999)[0-9]{4}$");
  public static final Pattern SSN7Regex = Pattern.compile("^[0-9](?!00)[0-9]{2}(?!0000|9999)[0-9]{4}$");
  public static final Pattern SSN6Regex = Pattern.compile("^(?!00)[0-9]{2}(?!0000|9999)[0-9]{4}$");
  public static final Pattern SSN5Regex = Pattern.compile("^[0-9](?!0000|9999)[0-9]{4}$");
  public static final Pattern SSN4Regex = Pattern.compile("^(?!0000|9999)[0-9]{4}$");

  /**
   * Regex to determine if an SSN is a series of 9 repeating digits.  This will be used to filter out SSNs that meet
   * this pattern, since 9 repeating digits is considered invalid.
   */
  public static final Pattern SSNRegexRepeating = Pattern.compile("^^(\\d)\\1{8}$");

  /**
   * The minimum size of the SSN field to consider it valid
   */
  public static final int MIN_SSN_LENGTH = 4;

  /**
   * Matches the patient identifier field against the following rules:
   * - Must have at least one alpha-numeric character
   * - May also (optionally) contain:
   *   - other alpha-numeric characters
   *   - space ( )
   *   - hyphen (-)
   *   - period (.)
   *   - number symbol (#)
   *   - underscore (_)
   */
  public static final Pattern PatientIDRegex = Pattern.compile("^[a-z0-9 \\-\\.\\#_]*[a-z0-9]+[a-z0-9 \\-\\.\\#_]*$", Pattern.CASE_INSENSITIVE);

  /**
   * Matches the name component against the following rules:
   * - Must have at least two alphabetic characters anywhere in the string
   * - All other characters are allowed
   */
  public static final Pattern NameRegex = Pattern.compile(".*[a-z]+.*[a-z]+.*", Pattern.CASE_INSENSITIVE);

  /**
   * SSNs that are considered invalid because they have been used in advertisements.
   * These must be formatted as 9 digits with no delimiters
   */
  public static final String[] BLACKLISTED_SSNS = new String[] {
    "078051120",
    "123456789"
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
    if (!row.containsKey(Engine.PATIENT_ID_FIELD) || ((String)row.get(Engine.PATIENT_ID_FIELD)).trim().isEmpty()) {
      missingBuilder.append("Patient Identifier, ");
      hasMissingError = true;
    }
    if (!row.containsKey(Engine.FIRST_NAME_FIELD) || ((String)row.get(Engine.FIRST_NAME_FIELD)).trim().isEmpty()) {
      missingBuilder.append("First Name, ");
      hasMissingError = true;
    }
    if (!row.containsKey(Engine.LAST_NAME_FIELD) || ((String)row.get(Engine.LAST_NAME_FIELD)).trim().isEmpty()) {
      missingBuilder.append("Last Name, ");
      hasMissingError = true;
    }
    if (!row.containsKey(Engine.DATE_OF_BIRTH_FIELD) || ((String)row.get(Engine.DATE_OF_BIRTH_FIELD)).trim().isEmpty()) {
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
    if (row.containsKey(Engine.FIRST_NAME_FIELD) && ((String)row.get(Engine.FIRST_NAME_FIELD)).trim().length() < Engine.MIN_NAME_LENGTH) {
      lengthBuilder.append("First Name, ");
      hasLengthError = true;
    }
    if (row.containsKey(Engine.LAST_NAME_FIELD) && ((String)row.get(Engine.LAST_NAME_FIELD)).trim().length() < Engine.MIN_NAME_LENGTH) {
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
    if (row.containsKey(Engine.DATE_OF_BIRTH_FIELD) && !isValidDateFormat((String)row.get(Engine.DATE_OF_BIRTH_FIELD))) {
      formatBuilder.append("Date of Birth (recommended to use MM/DD/YYYY format), ");
      hasFormatError = true;
    }
    if (row.containsKey(Engine.SOCIAL_SECURITY_NUMBER)) {
      String ssnString = (String)row.get(Engine.SOCIAL_SECURITY_NUMBER);
      if (shouldValidateSSNFormat(ssnString) && !isValidSSNFormat(ssnString)) {
        formatBuilder.append("Social Security Number, ");
        hasFormatError = true;
      }
    }
    if (row.containsKey(Engine.PATIENT_ID_FIELD)) {
      String patientIdString = (String)row.get(Engine.PATIENT_ID_FIELD);
      if (shouldValidatePatientIdentifierFormat(patientIdString) && !isValidPatientIdentifierFormat(patientIdString)){
        formatBuilder.append("Patient Identifier, ");
        hasFormatError = true;
      }
    }
    if (row.containsKey(Engine.FIRST_NAME_FIELD)) {
      String firstNameString = (String)row.get(Engine.FIRST_NAME_FIELD);
      if (shouldValidateNameFormat(firstNameString) && !isValidNameFormat(firstNameString)) {
        formatBuilder.append("First Name, ");
        hasFormatError = true;
      }
    }if (row.containsKey(Engine.LAST_NAME_FIELD)) {
      String lastNameString = (String)row.get(Engine.LAST_NAME_FIELD);
      if (shouldValidateNameFormat(lastNameString) && !isValidNameFormat(lastNameString)) {
        formatBuilder.append("Last Name, ");
        hasFormatError = true;
      }
    }

    if (hasFormatError) {
      row.setInvalidReason(safeAppendInvalidReason(row, formatBuilder.toString()));
    }

    return row;
  }

  /**
   * Utility method to perform a simple string format validation check against a regex
   * @param value
   * @param regex
   * @return
   */
  private boolean isStringValidAgainstRegexFormat(String value, Pattern regex) {
    if (value == null) {
      return false;
    }
    return regex.matcher(value).matches();
  }

  /**
   * Validate the name component against our format validation rules for names.
   * @param nameString
   * @return
   */
  public boolean isValidNameFormat(String nameString) {
    return isStringValidAgainstRegexFormat(nameString, NameRegex);
  }

  /**
   * Determine if the name component is long enough to warrant a format validation check
   * @param nameString
   * @return
   */
  private boolean shouldValidateNameFormat(String nameString) {
    return (nameString != null && nameString.trim().length() >= Engine.MIN_NAME_LENGTH);
  }

  /**
   * Because the SSN field is optional, this check will ensure that we are only doing a validation check when it is
   * appropriate.  Note that if we shouldn't validate the SSN, it should be explicitly blanked out later on.  This is
   * not the responsibility of the validation step to do, however.
   * @param ssnString
   * @return
   */
  private boolean shouldValidateSSNFormat(String ssnString) {
    return (ssnString != null) && (!ssnString.trim().equals(""));
  }

  /**
   * Determine if a string representing an SSN is valid or not.  Note that we are lumping TINs in with SSNs, although
   * they are slightly different.
   * @param ssnString
   * @return
   */
  public boolean isValidSSNFormat(String ssnString) {
    if (ssnString == null) {
      return false;
    }

    String strippedSSN = ssnString.trim().replaceAll("[\\- ]", "");
    if (strippedSSN.length() < MIN_SSN_LENGTH) {
      return false;
    }

    int strippedSSNLen = strippedSSN.length();

    // If it's beyond 9 characters, something is wrong - either too many digits or invalid delimiters.  Regardless,
    // we're done checking at this point.
    if (strippedSSNLen > 9) {
      return false;
    }
    // If it's 9 digits long, we need to check against our blacklist
    else if (strippedSSNLen == 9) {
      boolean blacklisted = Arrays.stream(BLACKLISTED_SSNS).anyMatch(x -> x.equals(strippedSSN));
      if (blacklisted) {
        return false;
      }

      // One edge condition not covered in our regex is starting with 666-**-****
      if (strippedSSN.startsWith("666")) {
        return false;
      }

      return SSN9Regex.matcher(strippedSSN).matches() && !SSNRegexRepeating.matcher(strippedSSN).matches();
    }
    else if (strippedSSNLen == 8) {
      return SSN8Regex.matcher(strippedSSN).matches();
    }
    else if (strippedSSNLen == 7) {
      return SSN7Regex.matcher(strippedSSN).matches();
    }
    else if (strippedSSNLen == 6) {
      return SSN6Regex.matcher(strippedSSN).matches();
    }
    else if (strippedSSNLen == 5) {
      return SSN5Regex.matcher(strippedSSN).matches();
    }
    else if (strippedSSNLen == 4) {
      return SSN4Regex.matcher(strippedSSN).matches();
    }

    return false;
  }

  /**
   * Determine if the patient identifier string is sufficient enough to perform a validation check against for
   * the format.
   * @param patientIdString
   * @return
   */
  private boolean shouldValidatePatientIdentifierFormat(String patientIdString) {
    return (patientIdString != null && !patientIdString.trim().isEmpty());
  }

  /**
   * Determine if a string representing a patient identifier is valid or not based on our formatting requirements.
   * @param patientIdString
   * @return
   */
  public boolean isValidPatientIdentifierFormat(String patientIdString) {
    return isStringValidAgainstRegexFormat(patientIdString, PatientIDRegex);
  }

  /**
   * Determine if a string, which should represent a date, is in one of our allowed input date formats.
   * @param dateString The string to parse
   * @return true if the string appears to be a date, false otherwise.
   */
  public boolean isValidDateFormat(String dateString) {
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

  @Override
  public void cleanup() {}
}
