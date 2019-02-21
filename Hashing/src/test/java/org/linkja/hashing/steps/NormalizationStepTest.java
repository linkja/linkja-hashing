package org.linkja.hashing.steps;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

//@Disabled("Disabled until we reconcile when to apply different normalization steps")
class NormalizationStepTest {
  private static ArrayList<String> prefixes = new ArrayList<String>() {{
    add("MISS ");
    add("MRS. ");
    add("MR. ");
    add("MR.-");
  }};

  private static ArrayList<String> suffixes = new ArrayList<String>() {{
    add(" III");
    add(" 1ST");
    add(" JR");
    add(" JR.");
    add("-JR.");
  }};

  @Test
  void normalizeString_Null() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertNull(step.normalizeString(null));
  }

  @Test
  void normalizeString_Empty() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("", step.normalizeString(""));
  }

  @Test
  void normalizeString_UpperCase() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("TEST DATA", step.normalizeString("test data"));
  }

  @Test
  void normalizeString_TrimWhitespace() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("TEST", step.normalizeString("   TEST  "));
    assertEquals("TEST", step.normalizeString("\tTEST\t"));
    assertEquals("TEST", step.normalizeString("\tTEST \t "));
  }

  @Test
  void removeExtraSeparators_ReplaceMultipleSpaces() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("this is a test", step.removeExtraSeparators("this  is  a  test"));
    assertEquals("this is a test", step.removeExtraSeparators("this   is   a   test"));
    assertEquals("this is a test", step.removeExtraSeparators("this    is a  test"));
  }

  @Test
  void removeExtraSeparators_ReplaceHyphens() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("test this", step.removeExtraSeparators("test--this"));
    assertEquals("test this", step.removeExtraSeparators("test-- this"));
    assertEquals("test this", step.removeExtraSeparators("test --this"));
    assertEquals("test this", step.removeExtraSeparators("test - - this"));
  }

  @Test
  void removeExtraSeparators_IntegrationTests() {
    // These tests are based off of sample data
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("MARS SMYTHE", step.removeExtraSeparators("\tMARS --SMYTHE\t "));
    assertEquals("SAROS ZOWATA", step.removeExtraSeparators("\tSAROS   ZOWATA\t "));
    assertEquals("EL JUNE AL ALAZ", step.removeExtraSeparators("\tEL JUNE   AL ALAZ\t "));
  }

  @Test
  void removeUnwantedCharacters_NullEmpty() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertNull(step.removeUnwantedCharacters(null));
    assertEquals("", step.removeUnwantedCharacters(""));
  }

  @Test
  void removeUnwantedCharacters_Replacements() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("a B c", step.removeUnwantedCharacters(" a B c 1 2 3 . \" -"));
  }

  @Test
  void removeUnwantedCharacters_NoReplacements() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("ABC onetwothree", step.removeUnwantedCharacters("ABC onetwothree"));
  }

  @Test
  void removePrefixes_NullEmpty() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals(null, step.removePrefixes(null));
    assertEquals("", step.removePrefixes(""));
  }

  @Test
  void removePrefixes_NoReplace() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("MR.", step.removePrefixes("MR."));
    assertEquals("MRSMITH", step.removePrefixes("MRSMITH"));
    assertEquals("MR.SMITH", step.removePrefixes("MR.SMITH"));
    assertEquals("MISSUNDERSTOOD", step.removePrefixes("MISSUNDERSTOOD"));
  }

  @Test
  void removePrefixes_SingleReplace() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("", step.removePrefixes("MR. "));
    assertEquals("SMITH", step.removePrefixes("MR. SMITH"));
    assertEquals("SMITH", step.removePrefixes("MR.-SMITH"));
    assertEquals("SMITH", step.removePrefixes("MRS. SMITH"));
    assertEquals("SMITH", step.removePrefixes("MISS SMITH"));
  }

  @Test
  void removePrefixes_MultiplePrefixesSingleReplace() {
    // Assumption is that we only remove the first one found
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("MRS. SMITH", step.removePrefixes("MR. MRS. SMITH"));
    assertEquals("MR. DR. SMITH", step.removePrefixes("MRS. MR. DR. SMITH"));
  }

  @Test
  void removeSuffixes_NullEmpty() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals(null, step.removeSuffixes(null));
    assertEquals("", step.removeSuffixes(""));
  }

  @Test
  void removeSuffixes_NoReplace() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("SMITH J R", step.removeSuffixes("SMITH J R"));
    assertEquals("SMITH.JR", step.removeSuffixes("SMITH.JR"));
    assertEquals("III", step.removeSuffixes("III"));
  }

  @Test
  void removeSuffixes_SingleReplace() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("SMITH", step.removeSuffixes("SMITH III"));
    assertEquals("SMITH", step.removeSuffixes("SMITH 1ST"));
    assertEquals("SMITH", step.removeSuffixes("SMITH JR"));
    assertEquals("SMITH", step.removeSuffixes("SMITH JR."));
    assertEquals("SMITH", step.removeSuffixes("SMITH-JR."));
  }

  @Test
  void removeSuffixes_MultipleSuffixesSingleReplace() {
    // Assumption is that we only remove the first one found
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("SMITH 1ST", step.removeSuffixes("SMITH 1ST JR"));
    assertEquals("SMITH JR", step.removeSuffixes("SMITH JR JR."));
  }

  @Test
  void run_NullEmpty() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals(null, step.run(null));

    DataRow row = new DataRow();
    assertEquals(0, step.run(row).size());
  }

  @Test
  void run_IntegrationTests() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    DataRow row = new DataRow() {{
      put(Engine.FIRST_NAME_FIELD, " MRS.   JANE ");
      put(Engine.PATIENT_ID_FIELD, " MRS.   JANE ");  // Set like first name to confirm we don't modify it
      put(Engine.LAST_NAME_FIELD, " SMITH III  ");
      put(Engine.SOCIAL_SECURITY_NUMBER, "123-45-6789");
      put(Engine.DATE_OF_BIRTH_FIELD, "6/13/1970");
    }};
    row = step.run(row);
    assertEquals("JANE", row.get(Engine.FIRST_NAME_FIELD));
    assertEquals(" MRS.   JANE ", row.get(Engine.PATIENT_ID_FIELD));
    assertEquals("SMITH", row.get(Engine.LAST_NAME_FIELD));
    assertEquals("6789", row.get(Engine.SOCIAL_SECURITY_NUMBER));
    assertEquals("1970-06-13", row.get(Engine.DATE_OF_BIRTH_FIELD));

    // The following block is a set of regression tests we developed with the SQL implementation to ensure consistent
    // output from both versions.
    row.put(Engine.FIRST_NAME_FIELD, "  MR. john  ");
    row = step.run(row);
    assertEquals("JOHN", row.get(Engine.FIRST_NAME_FIELD));
    row.put(Engine.FIRST_NAME_FIELD, "mr. JOHN  ");
    row = step.run(row);
    assertEquals("JOHN", row.get(Engine.FIRST_NAME_FIELD));
    row.put(Engine.FIRST_NAME_FIELD, "MR.-John-");
    row = step.run(row);
    assertEquals("JOHN", row.get(Engine.FIRST_NAME_FIELD));
    row.put(Engine.FIRST_NAME_FIELD, "  Mr. JOHN");
    row = step.run(row);
    assertEquals("JOHN", row.get(Engine.FIRST_NAME_FIELD));
    row.put(Engine.FIRST_NAME_FIELD, "    mR.   jOhN   ");
    row = step.run(row);
    assertEquals("JOHN", row.get(Engine.FIRST_NAME_FIELD));
    row.put(Engine.FIRST_NAME_FIELD, "MR. JOHN 17");
    row = step.run(row);
    assertEquals("JOHN", row.get(Engine.FIRST_NAME_FIELD));
  }

  @Test
  void normalizeSSN_NullEmpty() {
    NormalizationStep step = new NormalizationStep(null, null);
    assertNull(step.normalizeSSN(null));
    assertEquals("", step.normalizeSSN(""));
    assertEquals("", step.normalizeSSN("   "));
  }

  @Test
  void normalizeSSN_RemovesNonNumeric() {
    NormalizationStep step = new NormalizationStep(null, null);
    assertEquals("", step.normalizeSSN("ABCDEFG"));
    assertEquals("2345", step.normalizeSSN("ABCDEFG12345"));
    assertEquals("2345", step.normalizeSSN("ABCDEFG  12345"));
  }

  @Test
  void normalizeSSN_FlaggedInvalid() {
    NormalizationStep step = new NormalizationStep(null, null);
    assertEquals("", step.normalizeSSN("123 45 0000"));
  }

  @Test
  void normalizeSSN_BlankShortStrings() {
    NormalizationStep step = new NormalizationStep(null, null);
    assertEquals("", step.normalizeSSN("ABC"));
    assertEquals("", step.normalizeSSN("123"));
    assertEquals("", step.normalizeSSN("  123  "));
  }

  @Test
  void normalizeSSN_FullSSNConverted() {
    NormalizationStep step = new NormalizationStep(null, null);
    assertEquals("6789", step.normalizeSSN("123-45-6789"));
    assertEquals("6789", step.normalizeSSN("123456789"));
    assertEquals("6789", step.normalizeSSN("123 45 6789"));
    assertEquals("6789", step.normalizeSSN(" 1 2 3 4 5 6 7 8 9 "));
  }

  @Test
  void normalizeDate() {
    NormalizationStep step = new NormalizationStep(null, null);
    assertEquals("2019-01-01", step.normalizeDate("01/01/2019 13:30"));
    assertEquals("2019-12-31", step.normalizeDate("2019-12-31"));
  }
}