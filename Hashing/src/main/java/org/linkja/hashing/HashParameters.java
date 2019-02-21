package org.linkja.hashing;

import java.time.LocalDate;

public class HashParameters {
  private String siteId;
  private String siteName;
  private String privateSalt;
  private String projectSalt;
  private String projectId;
  private LocalDate privateDate;

  public String getSiteId() {
    return siteId;
  }

  public void setSiteId(String siteId) {
    this.siteId = siteId;
  }

  public String getSiteName() {
    return siteName;
  }

  public void setSiteName(String siteName) {
    this.siteName = siteName;
  }

  public String getPrivateSalt() {
    return privateSalt;
  }

  public void setPrivateSalt(String privateSalt) {
    this.privateSalt = privateSalt;
  }

  public String getProjectSalt() {
    return projectSalt;
  }

  public void setProjectSalt(String projectSalt) {
    this.projectSalt = projectSalt;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public LocalDate getPrivateDate() {
    return privateDate;
  }

  public void setPrivateDate(LocalDate privateDate) {
    this.privateDate = privateDate;
  }
}
