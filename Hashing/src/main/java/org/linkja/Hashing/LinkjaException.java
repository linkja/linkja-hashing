package org.linkja.Hashing;

/**
 * An exception that should be guaranteed to be sanitized of sensitive values, and can be displayed
 * directly to an end user.  Any part of the process that catches one of these exceptions should
 * display the message to the user.
 */
public class LinkjaException extends Exception {
  public LinkjaException(String message) {
    super(message);
  }
}
