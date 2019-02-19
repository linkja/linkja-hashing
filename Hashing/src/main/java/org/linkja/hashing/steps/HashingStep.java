package org.linkja.hashing.steps;

import org.linkja.hashing.DataRow;
import org.linkja.hashing.Engine;
import org.linkja.hashing.HashParameters;

public class HashingStep implements IStep {
  private HashParameters parameters;

  public HashingStep(HashParameters parameters) {
    this.parameters = parameters;
  }

  @Override
  public DataRow run(DataRow row) {
    //String field = String.format("%s%s%s%s", row.get(Engine.FIRST_NAME_FIELD));
    return row;
  }
}
