package org.linkja.hashing.steps;

import org.apache.commons.lang.StringUtils;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;

public class NormalizationStep implements IStep {
  public static final int MIN_SSN_LENGTH = 4;

  public static final DateTimeFormatter NORMALIZED_DATE_OF_BIRTH_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

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
    if (this.prefixes == null) {
      this.prefixes = prefixes;
    }
    if (this.suffixes == null) {
      this.suffixes = suffixes;
    }
  }

  @Override
  public DataRow run(DataRow row) {
    if (row == null || !row.shouldProcess()) {
      return row;
    }
    row.addCompletedStep(this.getStepName());

    for (Map.Entry<String, String> entry : row.entrySet()) {
      String fieldName = entry.getKey();
      String fieldValue = normalizeString(entry.getValue());
      // TODO - Determine if we need to make our processing more dynamic.
      // For now we will assume just the first and last name canonical fields get processed
      if (fieldName.equals(Engine.FIRST_NAME_FIELD)) {
        // Note that these functions are nested in a particular order to meet some input and output assumptions.
        // We need extra separators removed before we can remove prefixes and suffixes.  Once that is done, we
        // can safely remove remaining unwanted characters (e.g. because '.' is invalid, but is needed for prefix
        // detection).
        row.put(fieldName, removeUnwantedCharacters(
                removeSuffixes(removePrefixes(removeExtraSeparators(fieldValue))), false));
      }
      else if (fieldName.equals(Engine.LAST_NAME_FIELD)) {
        // Note that these functions are nested in a particular order to meet some input and output assumptions.
        // We need extra separators removed before we can remove prefixes and suffixes.  Once that is done, we
        // can safely remove remaining unwanted characters (e.g. because '.' is invalid, but is needed for prefix
        // detection).
        row.put(fieldName, removeUnwantedCharacters(
                removeSuffixes(removePrefixes(removeExtraSeparators(fieldValue))), true));
      }
      else if (fieldName.equals(Engine.SOCIAL_SECURITY_NUMBER)) {
        row.put(fieldName, normalizeString(normalizeSSN(fieldValue)));
      }
      else if (fieldName.equals(Engine.DATE_OF_BIRTH_FIELD)) {
        row.put(fieldName, normalizeString(normalizeDate(fieldValue)));
      }
      // TODO - Should we assume that every string passed in should be upper-cased and trimmed?
    }
    return row;
  }

  @Override
  public String getStepName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Collection of strings that we want to strip from a string and replace with a single space.
   * Replacement is done iteratively until none of these strings are found.
   */
  private static final String[] STRIP_STRING_COLLECTION = new String[] { "-", "  "};

  /**
   * After other processing steps have been completed, this specifies the characters to remove (which is defined as the
   * opposite of the characters we want to keep).
   */
  private static final String UNWANTED_CHARACTERS_PATTERN = "[^A-Za-z]";


  /**
   * After other processing steps have been completed, this specifies the characters to remove (which is defined as the
   * opposite of the characters we want to keep).
   */
  private static final String UNWANTED_CHARACTERS_WITH_SPACES_PATTERN = "[^A-Za-z ]";

  /**
   * String normalization steps include:
   *  - Trimming spaces
   *  - Converting to upper case
   * @param data The string data to normalize
   * @return
   */
  public String normalizeString(String data) {
    if (data == null) {
      return null;
    }
    return data.toUpperCase().trim();
  }

  /**
   * - Remove double spaces internally
   * - Replacing hyphens with a space
   * @param data
   * @return
   */
  public String removeExtraSeparators(String data) {
    if (data == null) {
      return null;
    }

    while (StringUtils.indexOfAny(data, STRIP_STRING_COLLECTION) >= 0) {
      for (String match : STRIP_STRING_COLLECTION) {
        data = data.replaceAll(match, " ");
      }
    }
    return data.trim();
  }

  /**
   * Remove from the parameter string all invalid characters, as defined within INVALID_CHARACTERS_PATTERN
   * @param data The string to remove unwanted characters from
   * @return An updated string (trimmed), or null if the input is null
   */
  public String removeUnwantedCharacters(String data, boolean allowSpaces) {
    if (data == null) {
      return null;
    }

    return data.replaceAll(allowSpaces ? UNWANTED_CHARACTERS_WITH_SPACES_PATTERN : UNWANTED_CHARACTERS_PATTERN, "").trim();
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

    return data.trim();
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

    return data.trim();
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

  /**
   * Normalize a date string into a canonical format.
   * @param date
   * @return
   */
  public String normalizeDate(String date) {
    return LocalDate.parse(date, ValidationFilterStep.DATE_OF_BIRTH_FORMATTER).format(NORMALIZED_DATE_OF_BIRTH_FORMAT);
  }
}
