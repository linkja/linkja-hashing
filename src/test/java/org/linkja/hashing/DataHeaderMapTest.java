package org.linkja.hashing;

import org.linkja.core.*;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class DataHeaderMapTest {

  @Test
  void createFromCSVHeader_Null() {
    DataHeaderMap map = new DataHeaderMap();
    assertNull(map.createFromCSVHeader(null));
  }

  @Test
  void createFromCSVHeader_Empty() {
    DataHeaderMap map = new DataHeaderMap();
    assertEquals(0, map.createFromCSVHeader(new HashMap<String, Integer>()).size());
  }

  @Test
  void createFromCSVHeader_Values() {
    DataHeaderMap map = new DataHeaderMap();
    HashMap<String, Integer> csvHeader = new HashMap<String, Integer>() {{
      put("test", 0);
      put("values", 1);
      put ("here", 2);
    }};
    map.createFromCSVHeader(csvHeader);
    assertEquals(csvHeader.size(), map.size());
    for (int index = 0; index < map.size(); index++) {
      DataHeaderMapEntry entry = map.get(index);
      assert(csvHeader.get(entry.getOriginalName()) == entry.getHeaderIndex());
      assertNull(entry.getCanonicalName());
    }
  }

  @Test
  void mergeCanonicalHeaders_Null() throws LinkjaException {
    DataHeaderMap map = new DataHeaderMap();
    assertEquals(map, map.mergeCanonicalHeaders(null));
  }

  @Test
  void mergeCanonicalHeaders_CalledBeforeCreate() throws LinkjaException {
    DataHeaderMap map = new DataHeaderMap();
    HashMap<String, String> canonicalHeader = new HashMap<String, String>() {{
      put("test", "test_0");
      put("values", "values_1");
      put ("here", "here_2");
    }};
    assertThrows(LinkjaException.class, () -> map.mergeCanonicalHeaders(canonicalHeader));
  }

  @Test
  void mergeCanonicalHeaders_Mismatch() throws LinkjaException {
    DataHeaderMap map = new DataHeaderMap();
    HashMap<String, Integer> csvHeader = new HashMap<String, Integer>() {{
      put("test", 0);
      put("values", 1);
      put("here", 2);
    }};
    HashMap<String, String> canonicalHeader = new HashMap<String, String>() {{
      put("tests", "test_0");
      put("value", "values_1");
      put("her", "here_2");
    }};

    map.createFromCSVHeader(csvHeader).mergeCanonicalHeaders(canonicalHeader);
    assertEquals(0, map.get(0).getHeaderIndex());
    assertEquals("test", map.get(0).getOriginalName());
    assertNull(map.get(0).getCanonicalName());
    assertEquals(1, map.get(1).getHeaderIndex());
    assertEquals("values", map.get(1).getOriginalName());
    assertNull(map.get(1).getCanonicalName());
    assertEquals(2, map.get(2).getHeaderIndex());
    assertEquals("here", map.get(2).getOriginalName());
    assertNull(map.get(2).getCanonicalName());
  }

  @Test
  void mergeCanonicalHeaders_AllMatches() throws LinkjaException {
    DataHeaderMap map = new DataHeaderMap();
    HashMap<String, Integer> csvHeader = new HashMap<String, Integer>() {{
      put("TEST", 0);
      put(" VALUES ", 1);
      put(" HeRe", 2);
    }};
    HashMap<String, String> canonicalHeader = new HashMap<String, String>() {{
      put("test", "test_0");
      put("values", "values_1");
      put("here", "here_2");
    }};
    map.createFromCSVHeader(csvHeader).mergeCanonicalHeaders(canonicalHeader);
    assertEquals(csvHeader.size(), map.size());
    for (int index = 0; index < map.size(); index++) {
      DataHeaderMapEntry entry = map.get(index);
      assert(csvHeader.get(entry.getOriginalName()) == entry.getHeaderIndex());
      assert(canonicalHeader.get(entry.getOriginalName().toLowerCase().trim()) == entry.getCanonicalName());
    }
  }

  @Test
  void findIndexOfCanonicalName_Null() throws NoSuchFieldException {
    HashMap<Integer, DataHeaderMapEntry> data = new HashMap<Integer, DataHeaderMapEntry>() {{
      put(1, new DataHeaderMapEntry(1, "test", "test_id"));
      put(2, new DataHeaderMapEntry(2, "other", "other_id"));
    }};
    DataHeaderMap map = new DataHeaderMap();
    FieldSetter.setField(map, map.getClass().getDeclaredField("entries"), data);
    assertEquals(-1, map.findIndexOfCanonicalName(null));
  }

  @Test
  void findIndexOfCanonicalName_NotFound() throws NoSuchFieldException {
    HashMap<Integer, DataHeaderMapEntry> data = new HashMap<Integer, DataHeaderMapEntry>() {{
      put(1, new DataHeaderMapEntry(1, "test", "test_id"));
      put(2, new DataHeaderMapEntry(2, "other", "other_id"));
    }};
    DataHeaderMap map = new DataHeaderMap();
    FieldSetter.setField(map, map.getClass().getDeclaredField("entries"), data);
    assertEquals(-1, map.findIndexOfCanonicalName("test_ids"));
  }

  @Test
  void findIndexOfCanonicalName_NotFoundNullEntries() throws NoSuchFieldException {
    HashMap<Integer, DataHeaderMapEntry> data = new HashMap<Integer, DataHeaderMapEntry>() {{
      put(1, new DataHeaderMapEntry(1, "test"));
      put(2, new DataHeaderMapEntry(2, "other"));
    }};
    DataHeaderMap map = new DataHeaderMap();
    FieldSetter.setField(map, map.getClass().getDeclaredField("entries"), data);
    assertEquals(-1, map.findIndexOfCanonicalName("test_ids"));
  }

  @Test
  void findIndexOfCanonicalName_Found() throws NoSuchFieldException {
    HashMap<Integer, DataHeaderMapEntry> data = new HashMap<Integer, DataHeaderMapEntry>() {{
      put(1, new DataHeaderMapEntry(1, "test", "test_id"));
      put(2, new DataHeaderMapEntry(2, "other", "other_id"));
    }};
    DataHeaderMap map = new DataHeaderMap();
    FieldSetter.setField(map, map.getClass().getDeclaredField("entries"), data);
    assertEquals(2, map.findIndexOfCanonicalName("other_id"));
  }

  @Test
  void containsCanonicalColumn_Null() throws NoSuchFieldException {
    HashMap<Integer, DataHeaderMapEntry> data = new HashMap<Integer, DataHeaderMapEntry>() {{
      put(1, new DataHeaderMapEntry(1, "test", "test_id"));
      put(2, new DataHeaderMapEntry(2, "other", "other_id"));
    }};
    DataHeaderMap map = new DataHeaderMap();
    FieldSetter.setField(map, map.getClass().getDeclaredField("entries"), data);
    assertFalse(map.containsCanonicalColumn(null));
  }

  @Test
  void containsCanonicalColumn_NotFound() throws NoSuchFieldException {
    HashMap<Integer, DataHeaderMapEntry> data = new HashMap<Integer, DataHeaderMapEntry>() {{
      put(1, new DataHeaderMapEntry(1, "test", "test_id"));
      put(2, new DataHeaderMapEntry(2, "other", "other_id"));
    }};
    DataHeaderMap map = new DataHeaderMap();
    FieldSetter.setField(map, map.getClass().getDeclaredField("entries"), data);
    assertFalse(map.containsCanonicalColumn("test_ids"));
  }

  @Test
  void containsCanonicalColumn_Found() throws NoSuchFieldException {
    HashMap<Integer, DataHeaderMapEntry> data = new HashMap<Integer, DataHeaderMapEntry>() {{
      put(1, new DataHeaderMapEntry(1, "test", "test_id"));
      put(2, new DataHeaderMapEntry(2, "other", "other_id"));
    }};
    DataHeaderMap map = new DataHeaderMap();
    FieldSetter.setField(map, map.getClass().getDeclaredField("entries"), data);
    assert(map.containsCanonicalColumn("other_id"));
  }
}