package org.linkja.hashing;

import org.linkja.hashing.steps.IStep;

import java.util.ArrayList;

/**
 * A record processor is a series of steps (IStep implementations) that are run on a row of data (DataRow).
 *
 * The RecordProcessor does not hold any state across each row of data.  This allows us to use the processor
 * in a guaranteed thread-safe manner without additional overhead.
 */
public class RecordProcessor {
  private ArrayList<IStep> processingSteps;

  /**
   * Creates an instance of our record processor, which is configured to execute a set of
   * processing steps.
   * @param steps The steps to run (in the order of execution)
   */
  public RecordProcessor(ArrayList<IStep> steps) {
    this.processingSteps = steps;
  }

  /**
   * Executes the configured series of instructions on a row of data
   * @param row The row of data to process.
   * @return The data row with contents modified after all processing steps are complete
   */
  public DataRow run(DataRow row) {
    for (IStep step : this.processingSteps) {
      if (step != null) {
        row = step.run(row);
      }
    }
    return row;
  }

  /**
   * Perform any necessary cleanup when all processing is complete.
   */
  public void cleanup() {
    for (IStep step : this.processingSteps) {
      if (step != null) {
        step.cleanup();
      }
    }
  }
}
