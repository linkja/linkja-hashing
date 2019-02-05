package org.linkja.Hashing;

import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EngineTest {

  @Test
  void normalizeHeader_NoChange() throws IOException {
    HashMap<String, Integer> map = new HashMap<String, Integer>() {{
      put("patient_id", 1);
      put("first_name", 2);
    }};
    Engine engine = new Engine(new EngineParameters());
    engine.initialize();
    Map<String, Integer> normalizedMap = engine.normalizeHeader(map);
    assertEquals(1, normalizedMap.get("patient_id").intValue());
    assertEquals(2, normalizedMap.get("first_name").intValue());
  }

  @Test
  void normalizeHeader_MixedCase() throws IOException {
    HashMap<String, Integer> map = new HashMap<String, Integer>() {{
      put("PatientID", 1);
      put("FirstName", 2);
    }};
    Engine engine = new Engine(new EngineParameters());
    engine.initialize();
    Map<String, Integer> normalizedMap = engine.normalizeHeader(map);
    assertEquals(1, normalizedMap.get("patient_id").intValue());
    assertEquals(2, normalizedMap.get("first_name").intValue());
  }

  @Test
  void normalizeHeader_Spaces() throws IOException {
    HashMap<String, Integer> map = new HashMap<String, Integer>() {{
      put("Patient ID", 1);
      put("First Name", 2);
    }};
    Engine engine = new Engine(new EngineParameters());
    engine.initialize();
    Map<String, Integer> normalizedMap = engine.normalizeHeader(map);
    assertEquals(1, normalizedMap.get("patient_id").intValue());
    assertEquals(2, normalizedMap.get("first_name").intValue());
  }

  @Test
  void normalizeHeader_PreserveUnknownFields() throws IOException {
    HashMap<String, Integer> map = new HashMap<String, Integer>() {{
      put("test", 1);
      put("Another", 2);
    }};
    Engine engine = new Engine(new EngineParameters());
    engine.initialize();
    Map<String, Integer> normalizedMap = engine.normalizeHeader(map);
    assertEquals(1, normalizedMap.get("test").intValue());
    assertEquals(2, normalizedMap.get("Another").intValue());
  }

  @Test
  void verifyFields_Null() throws NoSuchFieldException {
    Engine engine = new Engine(new EngineParameters());
    FieldSetter.setField(engine, engine.getClass().getDeclaredField("patientDataHeaderMap"), null);
    assertThrows(LinkjaException.class, () -> engine.verifyFields());
  }

  @Test
  void verifyFields_Empty() throws NoSuchFieldException {
    Engine engine = new Engine(new EngineParameters());
    FieldSetter.setField(engine, engine.getClass().getDeclaredField("patientDataHeaderMap"), new HashMap<String, Integer>());
    assertThrows(LinkjaException.class, () -> engine.verifyFields());
  }

  @Test
  void verifyFields_UnderThreshold() throws NoSuchFieldException {
    HashMap<String, Integer> map = new HashMap<String, Integer>() {{
      put("test", 1);
      put("Another", 2);
    }};
    Engine engine = new Engine(new EngineParameters());
    FieldSetter.setField(engine, engine.getClass().getDeclaredField("patientDataHeaderMap"), map);
    assertThrows(LinkjaException.class, () -> engine.verifyFields());
  }

  @Test
  void verifyFields_AtThreshold() throws Exception {
    HashMap<String, Integer> map = new HashMap<String, Integer>() {{
      put("test1", 1);
      put("test2", 2);
      put("test3", 3);
      put("test4", 4);
    }};
    Engine engine = new Engine(new EngineParameters());
    FieldSetter.setField(engine, engine.getClass().getDeclaredField("patientDataHeaderMap"), map);
    engine.verifyFields();  // Valid if no exception thrown
  }

  @Test
  void verifyFields_AboveThreshold() throws Exception {
    HashMap<String, Integer> map = new HashMap<String, Integer>() {{
      put("test1", 1);
      put("test2", 2);
      put("test3", 3);
      put("test4", 4);
      put("test5", 5);
    }};
    Engine engine = new Engine(new EngineParameters());
    FieldSetter.setField(engine, engine.getClass().getDeclaredField("patientDataHeaderMap"), map);
    engine.verifyFields();  // Valid if no exception thrown
  }
}