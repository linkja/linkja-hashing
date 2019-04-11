package org.linkja.hashing.steps;

import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;
import org.linkja.core.LinkjaException;

import java.util.HashMap;
import java.util.Map;

public class ExclusionStep implements IStep {
  public static final String PARTIAL_MATCH = "partial";
  public static final String EXACT_MATCH = "exact";

  private static HashMap<String,String> genericNames;

  /**
   * Creates an instance of the exception processor step, and provides the list of generic names to be considered
   * for flagging patients as an exception.
   * @param genericNames
   */
  public ExclusionStep(HashMap<String,String> genericNames) {
    if (this.genericNames == null) {
      this.genericNames = genericNames;
    }
  }

  @Override
  public DataRow run(DataRow row) {
    if (row == null || !row.shouldProcess()) {
      return row;
    }
    row.addCompletedStep(this.getStepName());

    // TODO - Determine how dynamic we should be.
    // For now, we assume just first name and last name should be processed by these rules
    row.setException(checkIsException(row.get(Engine.FIRST_NAME_FIELD), row.get(Engine.LAST_NAME_FIELD)));
    return row;
  }

  @Override
  public String getStepName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Determine if a name (based on first / last) matches our loaded patterns and rules for being an exception.
   * @param firstName The first name to check
   * @param lastName The last name to check
   * @return true if the first or last name appear to be in our exception list
   */
  public boolean checkIsException(String firstName, String lastName) {
    if (firstName == null || lastName == null) {
      throw new NullPointerException("The first name and last name cannot be null");
    }

    // Input for the list of generic names is assumed to be the base name (no flanking whitespace).  Using that, we will
    // check if either name starts with, ends with, or contains (with flanking whitespace) that name part
    for (Map.Entry<String,String> entry : this.genericNames.entrySet()) {
      String name = entry.getKey();
      String matchType = entry.getValue();
      if (matchType.equals(PARTIAL_MATCH)) {
        String startsWith = String.format("%s ", name);
        String contains = String.format(" %s ", name);
        String endsWith = String.format(" %s", name);
        if (namePartCheck(firstName, startsWith, contains, endsWith)
                || namePartCheck(lastName, startsWith, contains, endsWith)) {
          return true;
        }
      }
      else if (matchType.equals(EXACT_MATCH)) {
        if (firstName.equals(name) || lastName.equals(name)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Internal method to perform the partial name check.  Reduces duplicate code as we perform checks across multiple
   * names.
   * @param namePart
   * @param startsWith
   * @param contains
   * @param endsWith
   * @return
   */
  private boolean namePartCheck(String namePart, String startsWith, String contains, String endsWith) {
    if (namePart == null || namePart.equals("")) {
      return false;
    }

    return (namePart.startsWith(startsWith)
            || namePart.contains(contains)
            || namePart.endsWith(endsWith));
  }

  /**
   * Helper method to add a matching rule for exceptions to a collection.  This wraps up several of the validation
   * checks to ensure the rule is as expected.  Exceptions thrown will be formatted for display to the user to help
   * them troubleshoot and correct their file.
   * @param namePart
   * @param matchRule
   * @param recordNumber
   * @param ruleCollection
   * @throws LinkjaException
   * @return The modified collection, if the rule was added
   */
  public static void addMatchRuleToCollection(String namePart, String matchRule, long recordNumber, HashMap<String, String> ruleCollection) throws LinkjaException {
    if (ruleCollection == null) {
      throw new NullPointerException("The ruleCollection variable is null, and must be initialized before calling addMatchRuleToCollection");
    }

    if (namePart == null || namePart.trim().equals("")
    || matchRule == null || matchRule.trim().equals("")) {
      throw new LinkjaException(String.format("The generic-names.csv file used by LinkjaHashing has an invalid entry on line %d.  The name and match rule cannot be blank.", recordNumber));
    }

    matchRule = matchRule.trim().toLowerCase();
    if (!matchRule.equals(PARTIAL_MATCH) && !matchRule.equals(EXACT_MATCH)) {
      throw new LinkjaException(String.format("The generic-names.csv file used by LinkjaHashing has an invalid entry on line %d.  The match rule must be %s or %s.", recordNumber, PARTIAL_MATCH, EXACT_MATCH));
    }

    if (ruleCollection.containsKey(namePart)) {
      throw new LinkjaException(String.format("The generic-names.csv file used by LinkjaHashing has an invalid entry on line %d.  The name value %s has already been specified, and can only exist once in the file.", recordNumber, namePart));
    }

    ruleCollection.put(namePart, matchRule);
  }
}
