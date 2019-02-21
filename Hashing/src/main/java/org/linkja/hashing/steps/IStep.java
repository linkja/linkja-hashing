package org.linkja.hashing.steps;

import org.linkja.hashing.DataRow;
import org.linkja.hashing.LinkjaException;

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

  /**
   * Return a unique name for the step to identify which steps have been run.
   * @return
   */
  String getStepName();
}
