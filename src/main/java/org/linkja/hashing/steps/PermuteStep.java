package org.linkja.hashing.steps;

import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

public class PermuteStep implements IStep {
  private static final String LastNameSplitPattern = "-| ";


  @Override
  public DataRow run(DataRow row) {
    if (row == null || !row.shouldProcess()) {
      return row;
    }
    row.addCompletedStep(this.getStepName());

    row = permuteLastName(row);
    row.put(Engine.FIRST_NAME_FIELD, removeUnwantedCharacters((String)row.get(Engine.FIRST_NAME_FIELD)));

    return row;
  }

  @Override
  public String getStepName() {
    return this.getClass().getSimpleName();
  }

  /**
   * After the permutations have been completed, this is used to clean up the strings so that just alphabetic characters
   * remain.
   */
  private static final String INVALID_CHARACTERS_PATTERN = "[^A-Z]";

  /**
   * Given a last name, create permutations if there are multiple parts to the name.  This will retain the original
   * record as the main DataRow data, and create new derived DataRow objects.
   * e.g., SMITH-JONES
   *    DataRow LastName -> SMITH-JONES
   *       derivedRow[0] -> SMITH
   *       derivedRow[1] -> JONES
   * @param row The data row to process
   * @return The modified data row, or null if row is null.
   */
  public DataRow permuteLastName(DataRow row) {
    if (row == null) {
      return null;
    }

    // TODO - Determine how flexible we want to be
    // For now, assume we are only doing name permutations on the last name
    String lastName = (String)row.get(Engine.LAST_NAME_FIELD);
    if (lastName == null || lastName.equals("")) {
      return row;
    }

    String[] lastNameParts = lastName.split(LastNameSplitPattern);
    // If this isn't a multi-part name, we can stop further processing.
    if (lastNameParts.length <= 1) {
      return row;
    }

    // For now, we will only pull the first and last name parts, regardless of how
    // many there are.  This may be expanded in the future to handle more parts.
    String firstPart = removeUnwantedCharacters(lastNameParts[0]);
    String lastPart = removeUnwantedCharacters(lastNameParts[lastNameParts.length - 1]);

    // Clone the existing row (before we make any changes), for each of our new
    // name parts
    DataRow firstPartRow = (DataRow)row.clone();
    DataRow lastPartRow = (DataRow)row.clone();

    // Before adding each name part, make sure we only include them if it's at least as long as our minimum required
    // name.  This will allow "D C" as a last name, but "D" and "C" would not be derived last names.
    firstPartRow.put(Engine.LAST_NAME_FIELD, firstPart);
    if (((String)firstPartRow.get(Engine.LAST_NAME_FIELD)).length() >= Engine.MIN_NAME_LENGTH) {
      row.addDerivedRow(firstPartRow);
    }

    // For the 2nd part of the split name, make sure it isn't an exact match of the first (e.g., SMITH SMITH).  We
    // only want distinct permutations of the last name.
    lastPartRow.put(Engine.LAST_NAME_FIELD, lastPart);
    if (((String)lastPartRow.get(Engine.LAST_NAME_FIELD)).length() >= Engine.MIN_NAME_LENGTH
    && !firstPart.equals(lastPart)) {
      row.addDerivedRow(lastPartRow);
    }

    row.put(Engine.LAST_NAME_FIELD, removeUnwantedCharacters(lastName));

    return row;
  }

  /**
   * Remove from the parameter string all invalid characters, as defined within INVALID_CHARACTERS_PATTERN
   * @param data The string to remove unwanted characters from
   * @return An updated string (trimmed), or null if the input is null
   */
  public String removeUnwantedCharacters(String data) {
    if (data == null) {
      return null;
    }

    return data.toUpperCase().replaceAll(INVALID_CHARACTERS_PATTERN, "").trim();
  }

  @Override
  public void cleanup() {}
}
