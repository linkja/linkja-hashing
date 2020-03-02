package org.linkja.hashing.steps;

import org.apache.commons.lang.ArrayUtils;
import org.linkja.core.crypto.AesEncryptParameters;
import org.linkja.crypto.AesResult;
import org.linkja.crypto.Library;
import org.linkja.hashing.DataRow;

public class EncryptionStep implements IStep {
  // Specialty backup field for the PID hash.  Otherwise we will overwrite it
  // and won't be able to create our mapping file.
  public static final String PIDHASH_PLAINTEXT_FIELD = "PIDHASH_Plaintext";

  private AesEncryptParameters encryptParameters = null;

  public EncryptionStep(AesEncryptParameters encryptParameters) {
    this.encryptParameters = encryptParameters.clone();
  }

  @Override
  public DataRow run(DataRow row) {
    if (row == null || !row.shouldProcess()) {
      return row;
    }

    if (!row.hasCompletedStep(HashingStep.class.getSimpleName())) {
      row.setInvalidReason("The processing pipeline requires that Hashing be run before Encryption.");
      return row;
    }

    // Backup the PID Hash field
    row.put(PIDHASH_PLAINTEXT_FIELD, row.get(HashingStep.PIDHASH_FIELD));

    // If we are missing any of the required hashed fields, that's a problem.  We'll stop processing the row if that happens.
    for (String hashField : HashingStep.HASH_FIELDS) {
      if (!row.containsKey(hashField)) {
        if (HashingStep.REQUIRED_HASH_FIELDS.contains(hashField)) {
          row.setInvalidReason(String.format("Missing required hash field %s", hashField));
          return row;
        }
        continue;
      }

      encryptField(row, hashField);
    }

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

  private void encryptField(DataRow row, String hashField) {
    String hash = (String)row.get(hashField);
    AesResult encryptResult = Library.aesEncrypt(hash.getBytes(), this.encryptParameters.getAad(), this.encryptParameters.getKey(), this.encryptParameters.getIv());
    if (encryptResult == null) {
      row.setInvalidReason("Failed to encrypt");
      return;
    }
    row.put(hashField, ArrayUtils.addAll(encryptResult.data, encryptResult.tag));
  }

  @Override
  public void cleanup() {
    try {
      if (encryptParameters != null) {
        encryptParameters.clear();
      }
    }
    catch (Exception exc) {
      encryptParameters = null;
    }
  }

  @Override
  public String getStepName() {
    return this.getClass().getSimpleName();
  }
}
