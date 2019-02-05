package org.linkja.Hashing;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class Engine {
  public final int MIN_NUM_FIELDS = 4;

  private EngineParameters parameters;
  private static HashMap<String, String> canonicalHeaderNames = null;
  private Map<String, Integer> patientDataHeaderMap;

  public Engine(EngineParameters parameters) {
    setParameters(parameters);
  }

  public void initialize() throws IOException {
    if (this.canonicalHeaderNames == null) {
      canonicalHeaderNames = new HashMap<String, String>();
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource("configuration/canonical-header-names.csv").getFile());
      CSVParser parser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
      for (CSVRecord csvRecord : parser) {
        canonicalHeaderNames.put(csvRecord.get(0), csvRecord.get(1));
      }
    }

  }

  public void run() throws IOException {
    initialize();

    CSVParser parser = CSVParser.parse(parameters.getPatientFile(), Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
    this.patientDataHeaderMap = normalizeHeader(parser.getHeaderMap());
    for (CSVRecord csvRecord : parser) {
      System.out.println(csvRecord.get(1));
    }
  }

  public void verifyFields() throws LinkjaException {
    if (this.patientDataHeaderMap == null || this.patientDataHeaderMap.size() < MIN_NUM_FIELDS) {
      throw new LinkjaException("We require, at a minimum, columns for patient ID, first name, last name, and date of birth.\r\n" +
              "If you have these columns present, please make sure that the column header is specified, and that the column names are in our list of " +
              "recognized values.  Please see the project README for more information.");
    }
  }

  /**
   * Given the header entries for the CSV file, perform some quick normalization checks so that we can be a little
   * flexible in what users are allowed to send in.  We will preserve any column even if it isn't in our lookup list.
   * @param map
   * @return
   */
  public static Map<String, Integer> normalizeHeader(Map<String, Integer> map) {
    Map<String,Integer> normalizedMap = new HashMap<String, Integer>();
    for (Map.Entry<String,Integer> entry : map.entrySet()) {
      String csvField = entry.getKey();
      String csvFieldLower = csvField.toLowerCase();
      normalizedMap.put(canonicalHeaderNames.containsKey(csvFieldLower) ? canonicalHeaderNames.get(csvFieldLower) : csvField, entry.getValue());
    }
    return normalizedMap;
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
