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

  private static final DateTimeFormatter TRANSPOSE_DAY_MONTH_FORMAT = DateTimeFormatter.ofPattern("yyyy-dd-MM");

  private HashParameters parameters;
  private LocalDate convertedDateOfBirth;
  private MessageDigest messageDigest;

  public HashingStep(HashParameters parameters) {
    this.parameters = parameters;
  }

  @Override
  public DataRow run(DataRow row) {
    if (row == null) {
      return null;
    }

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

    // Only perform SSN hashes when SSN is set
    if (row.containsKey(Engine.SOCIAL_SECURITY_NUMBER) && row.get(Engine.SOCIAL_SECURITY_NUMBER).length() > 0) {
      row = fnamelnamedobssnHash(row);
    }
    return row;
  }

  @Override
  public String getStepName() {
    return this.getClass().getSimpleName();
  }

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
   *
   * @param row
   * @return
   */
  public DataRow patientIDHash(DataRow row) {
    long days = DAYS.between(this.convertedDateOfBirth, this.parameters.getPrivateDate());
    String hashInput = String.format("%s%s%d%s",
            row.get(Engine.PATIENT_ID_FIELD), this.parameters.getSiteId(), days, this.parameters.getPrivateSalt());
    row.put(PIDHASH_FIELD, getHashString(hashInput));
    return row;
  }

  public DataRow fnamelnamedobHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s",
            row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD), row.get(Engine.DATE_OF_BIRTH_FIELD),
            this.parameters.getPrivateSalt());
    row.put(FNAMELNAMEDOB_FIELD, getHashString(hashInput));
    return row;
  }

  public DataRow fnamelnamedobssnHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s",
            row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD), row.get(Engine.DATE_OF_BIRTH_FIELD),
            row.get(Engine.SOCIAL_SECURITY_NUMBER), this.parameters.getPrivateSalt());
    row.put(FNAMELNAMEDOBSSN_FIELD, getHashString(hashInput));
    return row;
  }

  public DataRow lnamefnamedobssnHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s",
            row.get(Engine.LAST_NAME_FIELD), row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.DATE_OF_BIRTH_FIELD),
            row.get(Engine.SOCIAL_SECURITY_NUMBER), this.parameters.getPrivateSalt());
    row.put(LNAMEFNAMEDOBSSN_FIELD, getHashString(hashInput));
    return row;
  }

  public DataRow lnamefnamedobHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s",
            row.get(Engine.LAST_NAME_FIELD), row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.DATE_OF_BIRTH_FIELD),
            this.parameters.getPrivateSalt());
    row.put(LNAMEFNAMEDOB_FIELD, getHashString(hashInput));
    return row;
  }

  public DataRow fnamelnameTdobssnHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s%s",
            row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD), this.convertedDateOfBirth.format(TRANSPOSE_DAY_MONTH_FORMAT),
            row.get(Engine.SOCIAL_SECURITY_NUMBER), this.parameters.getPrivateSalt());
    row.put(FNAMELNAMETDOBSSN_FIELD, getHashString(hashInput));
    return row;
  }

  public DataRow fnamelnameTdobHash(DataRow row) {
    String hashInput = String.format("%s%s%s%s",
            row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD), this.convertedDateOfBirth.format(TRANSPOSE_DAY_MONTH_FORMAT),
            this.parameters.getPrivateSalt());
    row.put(FNAMELNAMETDOB_FIELD, getHashString(hashInput));
    return row;
  }
}
