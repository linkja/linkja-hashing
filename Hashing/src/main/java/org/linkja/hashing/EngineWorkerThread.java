package org.linkja.hashing;

import org.linkja.hashing.steps.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Internal worker class to manage the actual engine
 */
public class EngineWorkerThread implements Callable<List<DataRow>> {
  private List<DataRow> dataRows;
  private boolean runNormalizationStep;
  private EngineParameters.RecordExclusionMode exclusionMode;
  private static ArrayList<String> prefixes = null;
  private static ArrayList<String> suffixes = null;
  private static HashMap<String, String> genericNames = null;
  private static HashParameters hashParameters;

  public EngineWorkerThread(List<DataRow> dataRows, boolean runNormalizationStep, EngineParameters.RecordExclusionMode exclusionMode,
                            ArrayList<String> prefixes, ArrayList<String> suffixes, HashMap<String, String> genericNames,
                            HashParameters hashParameters) {
    this.dataRows = new ArrayList<DataRow>(dataRows);
    this.runNormalizationStep = runNormalizationStep;
    this.exclusionMode = exclusionMode;
    this.prefixes = prefixes;
    this.suffixes = suffixes;
    this.genericNames = genericNames;
    this.hashParameters = hashParameters;
  }

  @Override
  public List<DataRow> call() throws Exception {
    ArrayList<IStep> steps = new ArrayList<IStep>();
    steps.add(new ValidationFilterStep());
    // The user can configure the system to skip normalization, if they prefer to handle that externally
    if (runNormalizationStep) {
      steps.add(new NormalizationStep(this.prefixes, this.suffixes));
    }
    // If the user doesn't want exception flags, or has already created them, we are going to skip this step in
    // the processing pipeline.
    // TODO - If the user said they have already provided the exception flag, should we confirm that?
    if (exclusionMode == EngineParameters.RecordExclusionMode.GenerateExclusions) {
      steps.add(new ExclusionStep(this.genericNames));
    }
    steps.add(new PermuteStep());
    steps.add(new HashingStep(this.hashParameters));

    RecordProcessor processor = new RecordProcessor(steps);
    List<DataRow> results = new ArrayList<DataRow>();
    for (DataRow row : this.dataRows) {
      results.add(processor.run(row));
    }
    return results;
  }
}