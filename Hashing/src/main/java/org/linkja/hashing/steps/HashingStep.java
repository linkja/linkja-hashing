package org.linkja.hashing.steps;

import org.bouncycastle.util.encoders.Hex;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;
import org.linkja.hashing.HashParameters;

import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

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

  private static final DateTimeFormatter TRANSPOSE_DAY_MONTH_FORMAT = DateTimeFormatter.ofPattern("yyyy-dd-MM");

  private HashParameters parameters;
  private LocalDate convertedDateOfBirth;
  private MessageDigest messageDigest;

  public HashingStep(HashParameters parameters) {
    this.parameters = parameters;
  }

  @Override
  public DataRow run(DataRow row) {
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
    row = fname3lnamedobHash(row);

    // Only perform SSN hashes when SSN is set
    if (row.containsKey(Engine.SOCIAL_SECURITY_NUMBER) && row.get(Engine.SOCIAL_SECURITY_NUMBER).length() > 0) {
      row = fnamelnamedobssnHash(row);
      row = lnamefnamedobssnHash(row);
      row = fnamelnamedobssnHash(row);
      row = fnamelnameTdobssnHash(row);
      row = fname3lnamedobssnHash(row);
      row = fnamelnamedobDssnHash(row);
      row = fnamelnamedobYssnHash(row);
    }

    // Process all of our derived rows
    if (row.hasDerivedRows()) {
      for (DataRow derivedRow : row.getDerivedRows()) {
        try {
          row.updateDerivedRow(derivedRow, run(derivedRow));
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
    this.convertedDateOfBirth = LocalDate.parse(row.get(Engine.DATE_OF_BIRTH_FIELD), NormalizationStep.NORMALIZED_DATE_OF_BIRTH_FORMAT);

    try {
      this.messageDigest = MessageDigest.getInstance("SHA-512");
    } catch (Exception e) {
      row.setInvalidReason(String.format("Failed to get an instance of the SHA-512 algorithm for hashing - %s",
              e.getMessage()));
    }
  }

  private String getHashString(String hashInput) {
    return Hex.toHexString(this.messageDigest.digest(hashInput.getBytes())).toUpperCase();
  }

  /**
   * Create a unique identifier hash using the patient's internal ID
   * @param row Data row to process
   * @return Modified data row with the PIDHASH_FIELD containing the hash
   */
  public DataRow patientIDHash(DataRow row) {
    long days = DAYS.between(this.convertedDateOfBirth, this.parameters.getPrivateDate());
    String hashInput = String.format("%s%s%d%s",
            row.get(Engine.PATIENT_ID_FIELD), this.parameters.getSiteId(), days, this.parameters.getPrivateSalt());
    row.put(PIDHASH_FIELD, getHashString(hashInput));
    return row;
  }

  /**
   * Create a hash of the first name, last name, and DOB
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMEDOB_FIELD containing the hash
   */
  public DataRow fnamelnamedobHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s",
            row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD), row.get(Engine.DATE_OF_BIRTH_FIELD),
            this.parameters.getPrivateSalt());
    row.put(FNAMELNAMEDOB_FIELD, getHashString(hashInput));
    return row;
  }

  /**
   * Create a hash of the first name, last name, DOB, and SSN
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMEDOBSSN_FIELD containing the hash
   */
  public DataRow fnamelnamedobssnHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s",
            row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD), row.get(Engine.DATE_OF_BIRTH_FIELD),
            row.get(Engine.SOCIAL_SECURITY_NUMBER), this.parameters.getPrivateSalt());
    row.put(FNAMELNAMEDOBSSN_FIELD, getHashString(hashInput));
    return row;
  }

  /**
   * Create a hash of the last name, first name, DOB, and SSN
   * @param row Data row to process
   * @return Modified data row with the LNAMEFNAMEDOBSSN_FIELD containing the hash
   */
  public DataRow lnamefnamedobssnHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s",
            row.get(Engine.LAST_NAME_FIELD), row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.DATE_OF_BIRTH_FIELD),
            row.get(Engine.SOCIAL_SECURITY_NUMBER), this.parameters.getPrivateSalt());
    row.put(LNAMEFNAMEDOBSSN_FIELD, getHashString(hashInput));
    return row;
  }

  /**
   * Create a hash of the last name, first name, and DOB
   * @param row Data row to process
   * @return Modified data row with the LNAMEFNAMEDOB_FIELD containing the hash
   */
  public DataRow lnamefnamedobHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s",
            row.get(Engine.LAST_NAME_FIELD), row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.DATE_OF_BIRTH_FIELD),
            this.parameters.getPrivateSalt());
    row.put(LNAMEFNAMEDOB_FIELD, getHashString(hashInput));
    return row;
  }

  /**
   * Create a hash of the first name, last name, transposed DOB (YYYY-DD-MM), and SSN
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMETDOBSSN_FIELD containing the hash
   */
  public DataRow fnamelnameTdobssnHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s",
            row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD), this.convertedDateOfBirth.format(TRANSPOSE_DAY_MONTH_FORMAT),
            row.get(Engine.SOCIAL_SECURITY_NUMBER), this.parameters.getPrivateSalt());
    row.put(FNAMELNAMETDOBSSN_FIELD, getHashString(hashInput));
    return row;
  }

  /**
   * Create a hash of the first name, last name, and transposed DOB (YYYY-DD-MM)
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMETDOB_FIELD containing the hash
   */
  public DataRow fnamelnameTdobHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s",
            row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD), this.convertedDateOfBirth.format(TRANSPOSE_DAY_MONTH_FORMAT),
            this.parameters.getPrivateSalt());
    row.put(FNAMELNAMETDOB_FIELD, getHashString(hashInput));
    return row;
  }

  /**
   * Create a hash of the first 3 characters of the first name, last name, DOB, and SSN
   * @param row Data row to process
   * @return Modified data row with the FNAME3LNAMEDOBSSN_FIELD containing the hash
   */
  public DataRow fname3lnamedobssnHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s",
            safeSubstring(row.get(Engine.FIRST_NAME_FIELD), 0, 3), row.get(Engine.LAST_NAME_FIELD), row.get(Engine.DATE_OF_BIRTH_FIELD),
            row.get(Engine.SOCIAL_SECURITY_NUMBER), this.parameters.getPrivateSalt());
    row.put(FNAME3LNAMEDOBSSN_FIELD, getHashString(hashInput));
    return row;
  }

  /**
   * Create a hash of the first 3 characters of the first name, last name, and DOB
   * @param row Data row to process
   * @return Modified data row with the FNAME3LNAMEDOB_FIELD containing the hash
   */
  public DataRow fname3lnamedobHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s",
            safeSubstring(row.get(Engine.FIRST_NAME_FIELD), 0, 3), row.get(Engine.LAST_NAME_FIELD), row.get(Engine.DATE_OF_BIRTH_FIELD),
            this.parameters.getPrivateSalt());
    row.put(FNAME3LNAMEDOB_FIELD, getHashString(hashInput));
    return row;
  }

  /**
   * Create a hash of the first name, last name, DOB + 1 day, and SSN
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMEDOBDSSN_FIELD containing the hash
   */
  public DataRow fnamelnamedobDssnHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s",
            row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD), this.convertedDateOfBirth.plusDays(1).format(NormalizationStep.NORMALIZED_DATE_OF_BIRTH_FORMAT),
            row.get(Engine.SOCIAL_SECURITY_NUMBER), this.parameters.getPrivateSalt());
    row.put(FNAMELNAMEDOBDSSN_FIELD, getHashString(hashInput));
    return row;
  }

  /**
   * Create a hash of the first name, last name, DOB + 1 year, and SSN
   * @param row Data row to process
   * @return Modified data row with the FNAMELNAMEDOBYSSN_FIELD containing the hash
   */
  public DataRow fnamelnamedobYssnHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s",
            row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD), this.convertedDateOfBirth.plusYears(1).format(NormalizationStep.NORMALIZED_DATE_OF_BIRTH_FORMAT),
            row.get(Engine.SOCIAL_SECURITY_NUMBER), this.parameters.getPrivateSalt());
    row.put(FNAMELNAMEDOBYSSN_FIELD, getHashString(hashInput));
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
}
