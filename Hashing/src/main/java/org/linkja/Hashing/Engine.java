package org.linkja.Hashing;

public class Engine {
  private EngineParameters parameters;

  public Engine(EngineParameters parameters) {
    setParameters(parameters);
  }

  public EngineParameters getParameters() {
    return parameters;
  }


  /**
   * Internally accessible method to set the parameters for running the engine.  Note that once the engine is
   * constructed, we don't want callers to be able to change execution parameters so this is private to the
   * class.
   * @param parameters The parameters needed to run this engine instance
   */
  private void setParameters(EngineParameters parameters) {
    this.parameters = parameters;
  }
}
