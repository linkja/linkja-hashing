package org.linkja.hashing;

public class DataHeaderMapEntry {
  private int headerIndex;
  private String originalName;
  private String canonicalName;

  public int getHeaderIndex() {
    return headerIndex;
  }

  public void setHeaderIndex(int headerIndex) {
    this.headerIndex = headerIndex;
  }

  public String getOriginalName() {
    return originalName;
  }

  public void setOriginalName(String originalName) {
    this.originalName = originalName;
  }

  public String getCanonicalName() {
    return canonicalName;
  }

  public void setCanonicalName(String canonicalName) {
    this.canonicalName = canonicalName;
  }

  public DataHeaderMapEntry() {
  }

  public DataHeaderMapEntry(int headerIndex, String originalName) {
    this.headerIndex = headerIndex;
    this.originalName = originalName;
  }

  public DataHeaderMapEntry(int headerIndex, String originalName, String canonicalName) {
    this.headerIndex = headerIndex;
    this.originalName = originalName;
    this.canonicalName = canonicalName;
  }
}
