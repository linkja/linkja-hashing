package org.linkja.hashing.steps;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

@Disabled("Disabled until we reconcile when to apply different normalization steps")
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
  void normalizeString_ReplaceMultipleSpaces() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("THIS IS A TEST", step.normalizeString("this  is  a  test"));
    assertEquals("THIS IS A TEST", step.normalizeString("this   is   a   test"));
    assertEquals("THIS IS A TEST", step.normalizeString("this    is a  test"));
  }

  @Test
  void normalizeString_ReplaceHyphens() {
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("TEST THIS", step.normalizeString("test--this"));
    assertEquals("TEST THIS", step.normalizeString("test-- this"));
    assertEquals("TEST THIS", step.normalizeString("test --this"));
    assertEquals("TEST THIS", step.normalizeString("test - - this"));
  }

  @Test
  void normalizeString_IntegrationTests() {
    // These tests are based off of sample data
    NormalizationStep step = new NormalizationStep(prefixes, suffixes);
    assertEquals("MARS SMYTHE", step.normalizeString("\tMARS --SMYTHE\t "));
    assertEquals("SAROS ZOWATA", step.normalizeString("\tSAROS   ZOWATA\t "));
    assertEquals("EL JUNE AL ALAZ", step.normalizeString("\tEL JUNE   AL ALAZ\t "));
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
      put(Engine.FIRST_NAME_FIELD, " MRS. JANE ");
      put(Engine.PATIENT_ID_FIELD, " MRS.   JANE ");
      put(Engine.LAST_NAME_FIELD, " SMITH III  ");
      put(Engine.SOCIAL_SECURITY_NUMBER, "123-45-6789");
    }};
    row = step.run(row);
    assertEquals("JANE", row.get(Engine.FIRST_NAME_FIELD));
    assertEquals(" MRS.   JANE ", row.get(Engine.PATIENT_ID_FIELD));
    assertEquals("SMITH", row.get(Engine.LAST_NAME_FIELD));
    assertEquals("6789", row.get(Engine.SOCIAL_SECURITY_NUMBER));
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
}