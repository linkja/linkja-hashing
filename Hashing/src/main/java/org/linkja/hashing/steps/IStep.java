package org.linkja.hashing.steps;

import org.linkja.hashing.DataRow;

/**
 * The Step interface defines a generic process that is run against a DataRow.
 */
public interface IStep {

  /**
   * Performs the specified action against a DataRow.
   * @param row The data row to act on
   * @returns DataRow modified by the process
   */
  DataRow run(DataRow row);
}
