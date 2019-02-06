package org.linkja.hashing;

import java.util.*;

public class DataHeaderMap {
  private HashMap<Integer, DataHeaderMapEntry> entries;

  public Collection<DataHeaderMapEntry> getEntries() {
    return entries.values();
  }

  public DataHeaderMap() {
    this.entries = new HashMap<Integer, DataHeaderMapEntry>();
  }

  public DataHeaderMap createFromCSVHeader(Map<String, Integer> csvHeader) {
    if (csvHeader == null) {
      return null;
    }

    for (Map.Entry<String, Integer> entry : csvHeader.entrySet()) {
      Integer index = entry.getValue();
      this.entries.put(index, new DataHeaderMapEntry(index, entry.getKey()));
    }

    return this;
  }

  public DataHeaderMap mergeCanonicalHeaders(Map<String, Integer> canonicalHeaders) throws LinkjaException {
    if (canonicalHeaders == null) {
      return this;
    }

    for (Map.Entry<String, Integer> entry : canonicalHeaders.entrySet()) {
      Integer index = entry.getValue();
      if (!this.entries.containsKey(index)) {
        throw new LinkjaException("We were unable to match up the CSV headers and the headers needed by the Hashing program.");
      }

      this.entries.get(index).setCanonicalName(entry.getKey());
    }

    return this;
  }

  /**
   * Given a canonical name, find the corresponding header index.
   * @param canonicalName The canonical name to search for
   * @return The header index, or -1 if the canonical name is not found
   */
  public int findIndexOfCanonicalName(String canonicalName) {
    if (this.entries == null) {
      return -1;
    }

    for (DataHeaderMapEntry entry : this.entries.values()) {
      if (entry.getCanonicalName().equals(canonicalName)) {
        return entry.getHeaderIndex();
      }
    }

    return -1;
  }

  /**
   * Determines if a canonical column name exists in the map
   * @param canonicalName The canonical name to search for
   * @return The header index, or -1 if the canonical name is not found
   */
  public boolean containsCanonicalColumn(String canonicalName) {
    return (findIndexOfCanonicalName(canonicalName) != -1);
  }

  public int size() {
    return this.entries.size();
  }

  public DataHeaderMapEntry get(int index) {
    return this.entries.get(index);
  }
}
