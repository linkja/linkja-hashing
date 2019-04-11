package org.linkja.hashing;

import org.linkja.core.*;
import java.util.*;

/**
 * A specialized collection that tracks the original data header from our input data source, and provides a mapping to
 * the appropriate canonical name.
 */
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

  /**
   * Merge in the canonical header mapping so that all of our entries have a mapped canonical header (if one exists).
   * @param canonicalHeaders
   * @return
   * @throws LinkjaException
   */
  public DataHeaderMap mergeCanonicalHeaders(Map<String, String> canonicalHeaders) throws LinkjaException {
    if (canonicalHeaders == null) {
      return this;
    }

    if (this.entries == null || this.entries.size() == 0) {
      throw new LinkjaException("You must load the list of headers before merging in the canonical headers");
    }

    for (Map.Entry<Integer, DataHeaderMapEntry> entry : this.entries.entrySet()) {
      Integer index = entry.getKey();
      String originalName = entry.getValue().getOriginalName().trim().toLowerCase();
      if (canonicalHeaders.containsKey(originalName)) {
        this.entries.get(index).setCanonicalName(canonicalHeaders.get(originalName));
      }
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
      if (entry.getCanonicalName() == null) {
        continue;
      }

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
