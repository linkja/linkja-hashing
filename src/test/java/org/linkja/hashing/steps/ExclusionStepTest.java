package org.linkja.hashing.steps;

import org.junit.jupiter.api.Test;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;
import org.linkja.hashing.LinkjaException;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class ExclusionStepTest {

  private static HashMap<String,String> genericNames = new HashMap<String,String>() {{
    put("BABY", ExclusionStep.PARTIAL_MATCH);
    put("VIP", ExclusionStep.EXACT_MATCH);
  }};

  @Test
  void checkIsException_NullEmpty() {
    ExclusionStep step = new ExclusionStep(genericNames);
    assertThrows(NullPointerException.class, () -> step.checkIsException(null, null));
    assertFalse(step.checkIsException("", ""));
  }

  @Test
  void checkIsException_Partial() {
    ExclusionStep step = new ExclusionStep(genericNames);
    assertFalse(step.checkIsException("BABY", "SMITH"));
    assert(step.checkIsException("BABY ONE", "SMITH"));
    assert(step.checkIsException("ONE BABY", "SMITH"));
    assert(step.checkIsException("NEW BABY HERE", "SMITH"));
    assertFalse(step.checkIsException("SWEETBABY", "ONE"));

    assertFalse(step.checkIsException("SWEET", "BABY"));
    assert(step.checkIsException("SWEET", "BABY ONE"));
    assert(step.checkIsException("SWEET", "NEW BABY ONE"));
    assert(step.checkIsException("SWEET", "ONE BABY"));
    assertFalse(step.checkIsException("ONE", "SWEETBABY"));
  }

  @Test
  void checkIsException_Exact() {
    ExclusionStep step = new ExclusionStep(genericNames);
    assert(step.checkIsException("VIP", "ONE"));
    assert(step.checkIsException("ONE", "VIP"));
    assertFalse(step.checkIsException("VIPER", "ONE"));
    assertFalse(step.checkIsException("ONE", "VIPER"));
    assertFalse(step.checkIsException("VIP ", "ONE"));
    assertFalse(step.checkIsException("ONE", "VIP "));
    assertFalse(step.checkIsException(" VIP", "ONE"));
    assertFalse(step.checkIsException("ONE", " VIP"));
  }

  @Test
  void addMatchRuleToCollection_NullEmpty() {
    assertThrows(NullPointerException.class, () -> ExclusionStep.addMatchRuleToCollection(null, null, 0, null));

    HashMap<String,String> genericNames = new HashMap<String,String>();
    assertThrows(LinkjaException.class, () -> ExclusionStep.addMatchRuleToCollection(null, "ok", 0, genericNames));
    assertThrows(LinkjaException.class, () -> ExclusionStep.addMatchRuleToCollection("ok", null, 0, genericNames));
    assertThrows(LinkjaException.class, () -> ExclusionStep.addMatchRuleToCollection("", "ok", 0, genericNames));
    assertThrows(LinkjaException.class, () -> ExclusionStep.addMatchRuleToCollection("ok", "", 0, genericNames));
    assertThrows(LinkjaException.class, () -> ExclusionStep.addMatchRuleToCollection("   ", "ok", 0, genericNames));
    assertThrows(LinkjaException.class, () -> ExclusionStep.addMatchRuleToCollection("ok", "   ", 0, genericNames));
  }

  @Test
  void addMatchRuleToCollection_ValidateMatchRule() throws LinkjaException {
    assertThrows(LinkjaException.class, () -> ExclusionStep.addMatchRuleToCollection("ok", "ok", 0, new HashMap<String,String>()));

    HashMap<String,String> genericNames = new HashMap<String,String>();
    ExclusionStep.addMatchRuleToCollection("ok", ExclusionStep.EXACT_MATCH, 0, genericNames);
    assertEquals(1, genericNames.size());
    genericNames = new HashMap<String,String>();

    ExclusionStep.addMatchRuleToCollection("ok", ExclusionStep.PARTIAL_MATCH, 0, genericNames);
    assertEquals(1, genericNames.size());
    genericNames = new HashMap<String,String>();

    ExclusionStep.addMatchRuleToCollection("ok", ExclusionStep.EXACT_MATCH.toUpperCase(), 0, genericNames);
    assertEquals(1, genericNames.size());
    genericNames = new HashMap<String,String>();

    ExclusionStep.addMatchRuleToCollection("ok", ExclusionStep.PARTIAL_MATCH.toUpperCase(), 0, genericNames);
    assertEquals(1, genericNames.size());
  }

  @Test
  void addMatchRuleToCollection_CheckForDuplicates() throws LinkjaException {
    HashMap<String,String> genericNames = new HashMap<String,String>();
    ExclusionStep.addMatchRuleToCollection("ok", ExclusionStep.EXACT_MATCH, 0, genericNames);
    ExclusionStep.addMatchRuleToCollection("ok ", ExclusionStep.EXACT_MATCH, 0, genericNames);  // Yes, whitespace differences are unique
    assertThrows(LinkjaException.class, () -> ExclusionStep.addMatchRuleToCollection("ok", ExclusionStep.EXACT_MATCH, 0, genericNames));
  }

  @Test
  void run_NullEmpty() {
    ExclusionStep step = new ExclusionStep(genericNames);
    assertEquals(null, step.run(null));

    DataRow row = new DataRow();
    assertThrows(NullPointerException.class, () -> step.run(row));
  }

  @Test
  void run_IntegrationTests() {
    ExclusionStep step = new ExclusionStep(genericNames);
    DataRow row = new DataRow() {{
      put(Engine.FIRST_NAME_FIELD, "BABY ");
      put(Engine.PATIENT_ID_FIELD, "123456");
      put(Engine.LAST_NAME_FIELD, "SMITH");
    }};
    assertFalse(row.isException());
    row = step.run(row);
    assert(row.isException());

    row = new DataRow() {{
      put(Engine.FIRST_NAME_FIELD, "JANE");
      put(Engine.PATIENT_ID_FIELD, "123456");
      put(Engine.LAST_NAME_FIELD, "SMITH");
    }};
    assertFalse(row.isException());
    row = step.run(row);
    assertFalse(row.isException());
  }

  @Test
  void run_TracksCompletedStep() {
    DataRow row = new DataRow() {{
      put(Engine.FIRST_NAME_FIELD, "BABY ");
      put(Engine.PATIENT_ID_FIELD, "123456");
      put(Engine.LAST_NAME_FIELD, "SMITH");
    }};

    ExclusionStep step = new ExclusionStep(genericNames);
    row = step.run(row);
    assert(row.hasCompletedStep(step.getStepName()));
  }

  @Test
  void run_DoesNotTrackCompletedStepWhenInvalid() {
    DataRow row = new DataRow() {{
      put(Engine.FIRST_NAME_FIELD, "BABY ");
      put(Engine.PATIENT_ID_FIELD, "123456");
      put(Engine.LAST_NAME_FIELD, "SMITH");
    }};

    row.setInvalidReason("Invalid for testing purposes");
    ExclusionStep step = new ExclusionStep(genericNames);
    row = step.run(row);
    assertFalse(row.hasCompletedStep(step.getStepName()));
  }
}