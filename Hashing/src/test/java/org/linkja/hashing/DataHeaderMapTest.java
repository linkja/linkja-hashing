package org.linkja.hashing;

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
    HashMap<String, Integer> canonicalHeader = new HashMap<String, Integer>() {{
      put("test", 0);
      put("values", 1);
      put ("here", 2);
    }};
    assertThrows(LinkjaException.class, () -> map.mergeCanonicalHeaders(canonicalHeader));
  }

  @Test
  void mergeCanonicalHeaders_Mismatch() throws LinkjaException {
    DataHeaderMap map = new DataHeaderMap();
    HashMap<String, Integer> csvHeader = new HashMap<String, Integer>() {{
      put("test", 0);
      put("values", 1);
      put ("here", 2);
    }};
    HashMap<String, Integer> canonicalHeader = new HashMap<String, Integer>() {{
      put("test", 1);
      put("values", 3);
      put ("here", 5);
    }};

    assertThrows(LinkjaException.class, () -> map.createFromCSVHeader(csvHeader).mergeCanonicalHeaders(canonicalHeader));
  }

  @Test
  void mergeCanonicalHeaders_AllMatches() throws LinkjaException {
    DataHeaderMap map = new DataHeaderMap();
    HashMap<String, Integer> csvHeader = new HashMap<String, Integer>() {{
      put("test", 0);
      put("values", 1);
      put ("here", 2);
    }};
    HashMap<String, Integer> canonicalHeader = new HashMap<String, Integer>() {{
      put("TEST", 0);
      put("VALUES", 1);
      put ("HERE", 2);
    }};
    map.createFromCSVHeader(csvHeader).mergeCanonicalHeaders(canonicalHeader);
    assertEquals(csvHeader.size(), map.size());
    for (int index = 0; index < map.size(); index++) {
      DataHeaderMapEntry entry = map.get(index);
      assert(csvHeader.get(entry.getOriginalName()) == entry.getHeaderIndex());
      assert(canonicalHeader.get(entry.getCanonicalName()) == entry.getHeaderIndex());
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