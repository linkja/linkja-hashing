package org.linkja.hashing.steps;

import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

public class PermuteStep implements IStep {
  private static final String LastNameSplitPattern = "-| ";


  @Override
  public DataRow run(DataRow row) {
    if (row == null) {
      return null;
    }

    row = permuteLastName(row);

    return row;
  }

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
    String lastName = row.get(Engine.LAST_NAME_FIELD);
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
    String firstPart = lastNameParts[0];
    String lastPart = lastNameParts[lastNameParts.length - 1];

    DataRow firstPartRow = (DataRow)row.clone();
    firstPartRow.put(Engine.LAST_NAME_FIELD, firstPart);
    row.addDerivedRow(firstPartRow);

    DataRow lastPartRow = (DataRow)row.clone();
    lastPartRow.put(Engine.LAST_NAME_FIELD, lastPart);
    row.addDerivedRow(lastPartRow);

    return row;
  }
}