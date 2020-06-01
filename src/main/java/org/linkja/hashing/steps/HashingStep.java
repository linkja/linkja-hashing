package org.linkja.hashing.steps;

import org.linkja.crypto.Library;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;
import org.linkja.hashing.HashParameters;

import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.time.temporal.ChronoUnit.DAYS;

public class HashingStep implements IStep {
  public static final String PIDHASH_FIELD = "PIDHASH";
  public static final String FNAMELNAMEDOBSSN_FIELD = "fnamelnamedobssn";
  public static final String FNAMELNAMEDOB_FIELD = "fnamelnamedob";
  public static final String LNAMEFNAMEDOBSSN_FIELD = "lnamefnamedobssn";
  public static final String LNAMEFNAMEDOB_FIELD = "lnamefnamedob";
  public static final String FNAMELNAMETDOBSSN_FIELD = "fnamelnameTdobssn";
  public static final String FNAMELNAMETDOB_FIELD = "fnamelnameTdob";
  public static final String FNAME3LNAMEDOBSSN_FIELD = "fname3lnamedobssn";
  public static final String FNAME3LNAMEDOB_FIELD = "fname3lnamedob";
  public static final String FNAMELNAMEDOBDSSN_FIELD = "fnamelnamedobDssn";
  public static final String FNAMELNAMEDOBYSSN_FIELD = "fnamelnamedobYssn";

  public static final List<String> HASH_FIELDS = new ArrayList<String>() {{
    add(PIDHASH_FIELD);
    add(FNAMELNAMEDOBSSN_FIELD);
    add(FNAMELNAMEDOB_FIELD);
    add(LNAMEFNAMEDOBSSN_FIELD);
    add(LNAMEFNAMEDOB_FIELD);
    add(FNAMELNAMETDOBSSN_FIELD);
    add(FNAMELNAMETDOB_FIELD);
    add(FNAME3LNAMEDOBSSN_FIELD);
    add(FNAME3LNAMEDOB_FIELD);
    add(FNAMELNAMEDOBDSSN_FIELD);
    add(FNAMELNAMEDOBYSSN_FIELD);
  }};

  public static final List<String> REQUIRED_HASH_FIELDS = new ArrayList<String>() {{
    add(PIDHASH_FIELD);
    add(FNAMELNAMEDOB_FIELD);
    add(LNAMEFNAMEDOB_FIELD);
    add(FNAMELNAMETDOB_FIELD);
  }};

  private static final DateTimeFormatter TRANSPOSE_DAY_MONTH_FORMAT = DateTimeFormatter.ofPattern("yyyy-dd-MM");

  private HashParameters parameters;
  private LocalDate convertedDateOfBirth;
  //private MessageDigest messageDigest;

  // Our commonly used field IDs will be cached in variables so that we can have fast access to them.  If we need this
  // to be more dynamic in the future (for fields we allow the user to configure), we can save a copy of the field IDs
  // and use those when generating hashes.
  private String firstNameFieldId = "";
  private String lastNameFieldId = "";
  private String dobFieldId = "";
  private String ssnFieldId = "";
  private String patientIdFieldId = "";
  private static final String privateSaltFieldId = "PVS";
  private static final String projectSaltFieldId = "PRS";
  private static final String siteIdFieldId = "SID";
  private static final String dateOffsetFieldId = "DOF";


  public HashingStep(HashParameters parameters, HashMap<String, String> fieldIds) {
    this.parameters = parameters;

    this.firstNameFieldId = fieldIds.get(Engine.FIRST_NAME_FIELD);
    this.lastNameFieldId = fieldIds.get(Engine.LAST_NAME_FIELD);
    this.dobFieldId = fieldIds.get(Engine.DATE_OF_BIRTH_FIELD);
    this.ssnFieldId = fieldIds.get(Engine.SOCIAL_SECURITY_NUMBER);
    this.patientIdFieldId = fieldIds.get(Engine.PATIENT_ID_FIELD);
  }

  @Override
  public DataRow run(DataRow row) {
    return calculateHashes(row, false);
  }

  /**
   * Internal worker method to calculate all of the hashes.  Pulled out of the run() method because we need the extra
   * parameter to flag if this is a derived row or not.
   * @param row
   * @param isDerivedRow
   * @return
   */
  private DataRow calculateHashes(DataRow row, boolean isDerivedRow) {
    if (row == null || !row.shouldProcess()) {
      return row;
    }
    row.addCompletedStep(this.getStepName());

    if (!row.hasCompletedStep(ValidationFilterStep.class.getSimpleName())) {
      row.setInvalidReason("The processing pipeline requires the Validation Filter to be run before Hashing.");
      return row;
    }

    cacheConvertedData(row);

    if (!row.shouldProcess()) {
      return row;
    }

    // We allow ourselves to assume the presence and correct format of all required fields at this point.
    row = patientIDHash(row);
    row = fnamelnamedobHash(row);
    row = lnamefnamedobHash(row);
    row = fnamelnameTdobHash(row);
    if (!isDerivedRow) {
      row = fname3lnamedobHash(row);
    }

    // Only perform SSN hashes when SSN is set
    if (row.containsKey(Engine.SOCIAL_SECURITY_NUMBER) && ((String)row.get(Engine.SOCIAL_SECURITY_NUMBER)).length() > 0) {
      row = fnamelnamedobssnHash(row);
      row = lnamefnamedobssnHash(row);
      row = fnamelnamedobssnHash(row);
      row = fnamelnameTdobssnHash(row);
      if (!isDerivedRow) {
        row = fname3lnamedobssnHash(row);
      }
      row = fnamelnamedobDssnHash(row);
      row = fnamelnamedobYssnHash(row);
    }

    // Process all of our derived rows
    if (row.hasDerivedRows()) {
      for (DataRow derivedRow : row.getDerivedRows()) {
        try {
          row.updateDerivedRow(derivedRow, calculateHashes(derivedRow, true));
        } catch (Exception e) {
          row.setInvalidReason(String.format("An exception was caught when trying to update a derived row. %s", e.getMessage()));
        }
      }
    }

    return row;
  }

  @Override
  public String getStepName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Utility method to allow caching of some conversions that we use multiple times in other methods.
   * @param row
   */
  public void cacheConvertedData(DataRow row) {
    // Cache this so it doesn't have to be re-parsed each time.
    this.convertedDateOfBirth = LocalDate.parse((String)row.get(Engine.DATE_OF_BIRTH_FIELD), NormalizationStep.NORMALIZED_DATE_OF_BIRTH_FORMAT);

//    try {
//      //this.messageDigest = MessageDigest.getInstance("SHA-512");
//    } catch (Exception e) {
//      row.setInvalidReason(String.format("Failed to get an instance of the SHA-512 algorithm for hashing - %s",
//              e.getMessage()));
//    }
  }

  private String getHashString(String hashInput) {
    return Library.hash(hashInput).toUpperCase();
  }

  /**
   * Formats the patient ID so that it may be hashed
   * @param row
   * @return
   */
  public String patientIDString(DataRow row) {
    long days = DAYS.between(this.convertedDateOfBirth, this.parameters.getPrivateDate());
    String hashInput = String.format("%s%s%s%s%s%d%s%s",
      this.patientIdFieldId, row.get(Engine.PATIENT_ID_FIELD),
      this.siteIdFieldId, this.parameters.getSiteId(),
      this.dateOffsetFieldId, days,
      this.privateSaltFieldId, this.parameters.getPrivateSalt());
    return hashInput;
  }

  /**
   * Create a unique identifier hash using the patient's internal ID
   * @param row Data row to process
   * @return Modified data row with the PIDHASH_FIELD containing the hash
   */
  public DataRow patientIDHash(DataRow row) {
    row.put(PIDHASH_FIELD, getHashString(patientIDString(row)));
    return row;
  }

  /**
   * Formats the first name, last name, and DOB for hashing
   * @param row
   * @return
   */
  public String fnamelnamedobString(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s%s%s%s",
      this.firstNameFieldId, row.get(Engine.FIRST_NAME_FIELD),
      this.lastNameFieldId, row.get(Engine.LAST_NAME_FIELD),
      this.dobFieldId, row.get(Engine.DATE_OF_BIRTH_FIELD),
      this.projectSaltFieldId, this.parameters.getProjectSalt());
    return hashInput;
  }

  /**
   * Create a hash of the first name, last name, and DOB
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMEDOB_FIELD containing the hash
   */
  public DataRow fnamelnamedobHash(DataRow row) {
    row.put(FNAMELNAMEDOB_FIELD, getHashString(fnamelnamedobString(row)));
    return row;
  }

  /**
   * Formats the first name, last name, DOB, and SSN for hashing
   * @param row
   * @return
   */
  public String fnamelnamedobssnString(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s%s%s%s%s%s",
      this.firstNameFieldId, row.get(Engine.FIRST_NAME_FIELD),
      this.lastNameFieldId, row.get(Engine.LAST_NAME_FIELD),
      this.dobFieldId, row.get(Engine.DATE_OF_BIRTH_FIELD),
      this.ssnFieldId, row.get(Engine.SOCIAL_SECURITY_NUMBER),
      this.projectSaltFieldId, this.parameters.getProjectSalt());
    return hashInput;
  }

  /**
   * Create a hash of the first name, last name, DOB, and SSN
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMEDOBSSN_FIELD containing the hash
   */
  public DataRow fnamelnamedobssnHash(DataRow row) {
    row.put(FNAMELNAMEDOBSSN_FIELD, getHashString(fnamelnamedobssnString(row)));
    return row;
  }

  /**
   * Format the last name, first name, DOB, and SSN for hashing
   * @param row
   * @return
   */
  public String lnamefnamedobssnString(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s%s%s%s%s%s",
      this.firstNameFieldId, row.get(Engine.LAST_NAME_FIELD),
      this.lastNameFieldId, row.get(Engine.FIRST_NAME_FIELD),
      this.dobFieldId, row.get(Engine.DATE_OF_BIRTH_FIELD),
      this.ssnFieldId, row.get(Engine.SOCIAL_SECURITY_NUMBER),
      this.projectSaltFieldId, this.parameters.getProjectSalt());
    return hashInput;
  }

  /**
   * Create a hash of the last name, first name, DOB, and SSN
   * @param row Data row to process
   * @return Modified data row with the LNAMEFNAMEDOBSSN_FIELD containing the hash
   */
  public DataRow lnamefnamedobssnHash(DataRow row) {
    row.put(LNAMEFNAMEDOBSSN_FIELD, getHashString(lnamefnamedobssnString(row)));
    return row;
  }

  /**
   * Format the last name, first name, and DOB for hashing
   * @param row
   * @return
   */
  public String lnamefnamedobString(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s%s%s%s",
      this.firstNameFieldId, row.get(Engine.LAST_NAME_FIELD),
      this.lastNameFieldId, row.get(Engine.FIRST_NAME_FIELD),
      this.dobFieldId, row.get(Engine.DATE_OF_BIRTH_FIELD),
      this.projectSaltFieldId, this.parameters.getProjectSalt());
    return hashInput;
  }

  /**
   * Create a hash of the last name, first name, and DOB
   * @param row Data row to process
   * @return Modified data row with the LNAMEFNAMEDOB_FIELD containing the hash
   */
  public DataRow lnamefnamedobHash(DataRow row) {
    row.put(LNAMEFNAMEDOB_FIELD, getHashString(lnamefnamedobString(row)));
    return row;
  }

  /**
   * Format the the first name, last name, transposed DOB (YYYY-DD-MM), and SSN for hashing
   * @param row
   * @return
   */
  public String fnamelnameTdobssnString(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s%s%s%s%s%s",
      this.firstNameFieldId, row.get(Engine.FIRST_NAME_FIELD),
      this.lastNameFieldId, row.get(Engine.LAST_NAME_FIELD),
      this.dobFieldId, this.convertedDateOfBirth.format(TRANSPOSE_DAY_MONTH_FORMAT),
      this.ssnFieldId, row.get(Engine.SOCIAL_SECURITY_NUMBER),
      this.projectSaltFieldId, this.parameters.getProjectSalt());
    return hashInput;
  }

  /**
   * Create a hash of the first name, last name, transposed DOB (YYYY-DD-MM), and SSN
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMETDOBSSN_FIELD containing the hash
   */
  public DataRow fnamelnameTdobssnHash(DataRow row) {
    row.put(FNAMELNAMETDOBSSN_FIELD, getHashString(fnamelnameTdobssnString(row)));
    return row;
  }

  /**
   * Format the first name, last name, and transposed DOB (YYYY-DD-MM) the hashing
   * @param row
   * @return
   */
  public String fnamelnameTdobString(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s%s%s%s",
      this.firstNameFieldId, row.get(Engine.FIRST_NAME_FIELD),
      this.lastNameFieldId, row.get(Engine.LAST_NAME_FIELD),
      this.dobFieldId, this.convertedDateOfBirth.format(TRANSPOSE_DAY_MONTH_FORMAT),
      this.projectSaltFieldId, this.parameters.getProjectSalt());
    return hashInput;
  }

  /**
   * Create a hash of the first name, last name, and transposed DOB (YYYY-DD-MM)
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMETDOB_FIELD containing the hash
   */
  public DataRow fnamelnameTdobHash(DataRow row) {
    row.put(FNAMELNAMETDOB_FIELD, getHashString(fnamelnameTdobString(row)));
    return row;
  }

  /**
   * Format the first 3 characters of the first name, last name, DOB, and SSN for hashing
   * @param row
   * @return
   */
  public String fname3lnamedobssnString(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s%s%s%s%s%s",
      this.firstNameFieldId, safeSubstring((String)row.get(Engine.FIRST_NAME_FIELD), 0, 3),
      this.lastNameFieldId, row.get(Engine.LAST_NAME_FIELD),
      this.dobFieldId, row.get(Engine.DATE_OF_BIRTH_FIELD),
      this.ssnFieldId, row.get(Engine.SOCIAL_SECURITY_NUMBER),
      this.projectSaltFieldId, this.parameters.getProjectSalt());
    return hashInput;
  }

  /**
   * Create a hash of the first 3 characters of the first name, last name, DOB, and SSN
   * @param row Data row to process
   * @return Modified data row with the FNAME3LNAMEDOBSSN_FIELD containing the hash
   */
  public DataRow fname3lnamedobssnHash(DataRow row) {
    row.put(FNAME3LNAMEDOBSSN_FIELD, getHashString(fname3lnamedobssnString(row)));
    return row;
  }

  /**
   * Format the first 3 characters of the first name, last name, and DOB for hashing
   * @param row
   * @return
   */
  public String fname3lnamedobString(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s%s%s%s",
      this.firstNameFieldId, safeSubstring((String)row.get(Engine.FIRST_NAME_FIELD), 0, 3),
      this.lastNameFieldId, row.get(Engine.LAST_NAME_FIELD),
      this.dobFieldId, row.get(Engine.DATE_OF_BIRTH_FIELD),
      this.projectSaltFieldId, this.parameters.getProjectSalt());
    return hashInput;
  }

  /**
   * Create a hash of the first 3 characters of the first name, last name, and DOB
   * @param row Data row to process
   * @return Modified data row with the FNAME3LNAMEDOB_FIELD containing the hash
   */
  public DataRow fname3lnamedobHash(DataRow row) {
    row.put(FNAME3LNAMEDOB_FIELD, getHashString(fname3lnamedobString(row)));
    return row;
  }

  /**
   * Format the first name, last name, DOB + 1 day, and SSN for hashing
   * @param row
   * @return
   */
  public String fnamelnamedobDssnString(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s%s%s%s%s%s",
      this.firstNameFieldId, row.get(Engine.FIRST_NAME_FIELD),
      this.lastNameFieldId, row.get(Engine.LAST_NAME_FIELD),
      this.dobFieldId, this.convertedDateOfBirth.plusDays(1).format(NormalizationStep.NORMALIZED_DATE_OF_BIRTH_FORMAT),
      this.ssnFieldId, row.get(Engine.SOCIAL_SECURITY_NUMBER),
      this.projectSaltFieldId, this.parameters.getProjectSalt());
    return hashInput;
  }

  /**
   * Create a hash of the first name, last name, DOB + 1 day, and SSN
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMEDOBDSSN_FIELD containing the hash
   */
  public DataRow fnamelnamedobDssnHash(DataRow row) {
    row.put(FNAMELNAMEDOBDSSN_FIELD, getHashString(fnamelnamedobDssnString(row)));
    return row;
  }

  /**
   * Return the first name, last name, DOB + 1 year, and SSN for hashing
   * @param row
   * @return
   */
  public String fnamelnamedobYssnString(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s%s%s%s%s%s",
      this.firstNameFieldId, row.get(Engine.FIRST_NAME_FIELD),
      this.lastNameFieldId, row.get(Engine.LAST_NAME_FIELD),
      this.dobFieldId, this.convertedDateOfBirth.plusYears(1).format(NormalizationStep.NORMALIZED_DATE_OF_BIRTH_FORMAT),
      this.ssnFieldId, row.get(Engine.SOCIAL_SECURITY_NUMBER),
      this.projectSaltFieldId, this.parameters.getProjectSalt());
    return hashInput;
  }

  /**
   * Create a hash of the first name, last name, DOB + 1 year, and SSN
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMEDOBYSSN_FIELD containing the hash
   */
  public DataRow fnamelnamedobYssnHash(DataRow row) {
    row.put(FNAMELNAMEDOBYSSN_FIELD, getHashString(fnamelnamedobYssnString(row)));
    return row;
  }

  /**
   * Utility method to get a substring, starting at a 0-based index, and collecting no more than length characters.
   * This is a "safe" variant, because it will handle the checks if the length parameter exceeds the length of the
   * string.  If invalid parameters (e.g., negative numbers) are passed in, this will still throw an exception.
   * @param string String to process
   * @param start The 0-based index to start at
   * @param length The number of characters to extract, including the start position
   * @return Substring requested
   */
  public static String safeSubstring(String string, int start, int length) {
    if (string == null || string.length() == 0) {
      return string;
    }

    if ((start + length) >= string.length()) {
      return string.substring(start);
    }
    else {
      return string.substring(start, (start + length));
    }
  }

  @Override
  public void cleanup() {}
}
