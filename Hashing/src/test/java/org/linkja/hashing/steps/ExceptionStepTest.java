package org.linkja.hashing.steps;

import org.junit.jupiter.api.Test;
import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;
import org.linkja.hashing.LinkjaException;

import java.util.ArrayList;
import java.util.HashMap;

import static javax.swing.UIManager.put;
import static org.junit.jupiter.api.Assertions.*;

class ExceptionStepTest {

  private static HashMap<String,String> genericNames = new HashMap<String,String>() {{
    put("BABY", ExceptionStep.PARTIAL_MATCH);
    put("VIP", ExceptionStep.EXACT_MATCH);
  }};

  @Test
  void checkIsException_NullEmpty() {
    ExceptionStep step = new ExceptionStep(genericNames);
    assertThrows(NullPointerException.class, () -> step.checkIsException(null, null));
    assertFalse(step.checkIsException("", ""));
  }

  @Test
  void checkIsException_Partial() {
    ExceptionStep step = new ExceptionStep(genericNames);
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
    ExceptionStep step = new ExceptionStep(genericNames);
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
    assertThrows(NullPointerException.class, () -> ExceptionStep.addMatchRuleToCollection(null, null, 0, null));

    HashMap<String,String> genericNames = new HashMap<String,String>();
    assertThrows(LinkjaException.class, () -> ExceptionStep.addMatchRuleToCollection(null, "ok", 0, genericNames));
    assertThrows(LinkjaException.class, () -> ExceptionStep.addMatchRuleToCollection("ok", null, 0, genericNames));
    assertThrows(LinkjaException.class, () -> ExceptionStep.addMatchRuleToCollection("", "ok", 0, genericNames));
    assertThrows(LinkjaException.class, () -> ExceptionStep.addMatchRuleToCollection("ok", "", 0, genericNames));
    assertThrows(LinkjaException.class, () -> ExceptionStep.addMatchRuleToCollection("   ", "ok", 0, genericNames));
    assertThrows(LinkjaException.class, () -> ExceptionStep.addMatchRuleToCollection("ok", "   ", 0, genericNames));
  }

  @Test
  void addMatchRuleToCollection_ValidateMatchRule() throws LinkjaException {
    assertThrows(LinkjaException.class, () -> ExceptionStep.addMatchRuleToCollection("ok", "ok", 0, new HashMap<String,String>()));

    HashMap<String,String> genericNames = new HashMap<String,String>();
    ExceptionStep.addMatchRuleToCollection("ok", ExceptionStep.EXACT_MATCH, 0, genericNames);
    assertEquals(1, genericNames.size());
    genericNames = new HashMap<String,String>();

    ExceptionStep.addMatchRuleToCollection("ok", ExceptionStep.PARTIAL_MATCH, 0, genericNames);
    assertEquals(1, genericNames.size());
    genericNames = new HashMap<String,String>();

    ExceptionStep.addMatchRuleToCollection("ok", ExceptionStep.EXACT_MATCH.toUpperCase(), 0, genericNames);
    assertEquals(1, genericNames.size());
    genericNames = new HashMap<String,String>();

    ExceptionStep.addMatchRuleToCollection("ok", ExceptionStep.PARTIAL_MATCH.toUpperCase(), 0, genericNames);
    assertEquals(1, genericNames.size());
  }

  @Test
  void addMatchRuleToCollection_CheckForDuplicates() throws LinkjaException {
    HashMap<String,String> genericNames = new HashMap<String,String>();
    ExceptionStep.addMatchRuleToCollection("ok", ExceptionStep.EXACT_MATCH, 0, genericNames);
    ExceptionStep.addMatchRuleToCollection("ok ", ExceptionStep.EXACT_MATCH, 0, genericNames);  // Yes, whitespace differences are unique
    assertThrows(LinkjaException.class, () -> ExceptionStep.addMatchRuleToCollection("ok", ExceptionStep.EXACT_MATCH, 0, genericNames));
  }

  @Test
  void run_NullEmpty() {
    ExceptionStep step = new ExceptionStep(genericNames);
    assertEquals(null, step.run(null));

    DataRow row = new DataRow();
    assertThrows(NullPointerException.class, () -> step.run(row));
  }

  @Test
  void run_IntegrationTests() {
    ExceptionStep step = new ExceptionStep(genericNames);
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
}