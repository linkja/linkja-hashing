package org.linkja.hashing.steps;

import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

import java.util.ArrayList;
import java.util.Map;

public class NormalizationStep implements IStep {
  public static final int MIN_SSN_LENGTH = 4;

  // This is a specific SSN that is of appropriate length, but is considered an invalid placeholder and should be
  // removed
  public static final String INVALID_SSN = "0000";

  private static ArrayList<String> prefixes = null;
  private static ArrayList<String> suffixes = null;

  /**
   * Creates an instance of the NormalizationStep
   * @param prefixes A list of prefixes used by the normalization step
   * @param suffixes A list of suffixes used by the normalization step
   */
  public NormalizationStep(ArrayList<String> prefixes, ArrayList<String> suffixes) {
    this.prefixes = prefixes;
    this.suffixes = suffixes;
  }

  @Override
  public DataRow run(DataRow row) {
    if (row == null) {
      return null;
    }

    for (Map.Entry<String, String> entry : row.entrySet()) {
      String fieldName = entry.getKey();
      // TODO - Determine if we need to make our processing more dynamic.
      // For now we will assume just the first and last name canonical fields get processed
      if (fieldName.equals(Engine.FIRST_NAME_FIELD) || fieldName.equals(Engine.LAST_NAME_FIELD)) {
        row.put(fieldName, normalizeString(
                removeSuffixes(removePrefixes(entry.getValue()))));
      }
      else if (fieldName.equals(Engine.SOCIAL_SECURITY_NUMBER)) {
        row.put(fieldName, normalizeSSN(entry.getValue()));
      }
    }
    return row;
  }

  /**
   * String normalization steps include:
   *  - Trimming spaces
   *  - Converting to upper case
   *  - Remove double spaces internally
   *  X- Replacing hyphens with a space
   * @param data The string data to normalize
   * @return
   */
  public String normalizeString(String data) {
    if (data == null) {
      return null;
    }
    return data
            .toUpperCase()
            //.replaceAll("-", " ")
            .replaceAll("[ ]{2,}", " ")
            .trim();
  }

  /**
   * Removes the first instance of a prefix from the supplied string.
   * @param data The string to remove a prefix from
   * @return The modified string, or null if a null string is provided
   */
  public String removePrefixes(String data) {
    if (data == null) {
      return null;
    }

    for (String prefix : this.prefixes) {
      if (data.startsWith(prefix)) {
        data = data.substring(prefix.length());
        break;
      }
    }

    return data;
  }

  /**
   * Removes the first instance of a suffix from the supplied string.
   * @param data The string to remove a suffix from
   * @return The modified string, or null if a null string is provided
   */
  public String removeSuffixes(String data) {
    if (data == null) {
      return null;
    }

    for (String suffix : this.suffixes) {
      if (data.endsWith(suffix)) {
        data = data.substring(0, data.length() - suffix.length());
        break;
      }
    }

    return data;
  }

  /**
   * Normalizes the social security number using the following rules:
   *  - Strip all non-numeric characters
   *  - If length >= 4, return rightmost 4 characters, else return empty string
   * @param data
   * @return
   */
  public String normalizeSSN(String data) {
    if (data == null) {
      return null;
    }

    String numericChars = data.replaceAll("[^0-9]", "");
    if (numericChars == null || numericChars.length() >= MIN_SSN_LENGTH) {
      numericChars = numericChars.substring(numericChars.length() - MIN_SSN_LENGTH);
      if (!numericChars.equals(INVALID_SSN)) {
        return numericChars;
      }
    }

    return "";
  }
}
