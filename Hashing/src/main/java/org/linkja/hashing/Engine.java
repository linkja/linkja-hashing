package org.linkja.hashing;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.linkja.hashing.steps.IStep;
import org.linkja.hashing.steps.NormalizationStep;
import org.linkja.hashing.steps.ValidationFilterStep;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class Engine {
  public static final int MIN_NUM_FIELDS = 4;

  // List the canonical names for required fields
  public static final String PATIENT_ID_FIELD = "patient_id";
  public static final String FIRST_NAME_FIELD = "first_name";
  public static final String LAST_NAME_FIELD = "last_name";
  public static final String DATE_OF_BIRTH_FIELD = "date_of_birth";

  private static final String REQUIRED_COLUMNS_EXCEPTION_MESSAGE = "We require, at a minimum, columns for patient ID, first name, last name, and date of birth.\r\n" +
          "If you have these columns present, please make sure that the column header is specified, and that the column names are in our list of " +
          "recognized values.  Please see the project README for more information.";

  private EngineParameters parameters;
  private static HashMap<String, String> canonicalHeaderNames = null;
  private DataHeaderMap patientDataHeaderMap;

  public Engine(EngineParameters parameters) {
    setParameters(parameters);
    this.patientDataHeaderMap = new DataHeaderMap();
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

  /**
   * Internal conversion method to take a CSVRecord and convert it to a canoncial DataRow structure
   * @param csvRecord The CSVRecord to conver
   * @return A DataRow representing the data
   */
  public DataRow csvRecordToDataRow(CSVRecord csvRecord) {
    DataRow row = new DataRow();
    for (DataHeaderMapEntry entry : this.patientDataHeaderMap.getEntries()) {
      row.put(entry.getCanonicalName(), csvRecord.get(entry.getOriginalName()));
    }
    row.setRowNumber(csvRecord.getRecordNumber());
    return row;
  }

  public void run() throws IOException, LinkjaException {
    initialize();

    CSVParser parser = CSVParser.parse(parameters.getPatientFile(), Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
    Map<String, Integer> csvHeaderMap = parser.getHeaderMap();
    this.patientDataHeaderMap.createFromCSVHeader(csvHeaderMap).mergeCanonicalHeaders(normalizeHeader(csvHeaderMap));
    verifyFields();

    HashSet<String> uniquePatientIds = new HashSet<>();
    int patientIdIndex = this.patientDataHeaderMap.findIndexOfCanonicalName(PATIENT_ID_FIELD);

    ArrayList<IStep> steps = new ArrayList<IStep>();
    steps.add(new ValidationFilterStep());
    steps.add(new NormalizationStep());

    // Because our check for unique patient IDs requires knowing every ID that has been seen, and because our processing
    // steps do not hold state, we are performing this check as we load up our worker queue.
    for (CSVRecord csvRecord : parser) {
      String patientId = csvRecord.get(patientIdIndex);
      if (uniquePatientIds.contains(patientId)) {
        throw new LinkjaException(String.format("Patient IDs must be unique within the data file.  A duplicate copy of Patient ID %s was found on row %ld.",
                patientId, csvRecord.getRecordNumber()));
      }
      uniquePatientIds.add(patientId);

      DataRow row = csvRecordToDataRow(csvRecord);
      RecordProcessor processor = new RecordProcessor(steps);
      processor.run(row);
    }
  }

  /**
   * Ensures that the data fields loaded meet our minimum expectations regarding number of fields, and inclusion of
   * required fields
   * @throws LinkjaException
   */
  public void verifyFields() throws LinkjaException {
    if (this.patientDataHeaderMap == null || this.patientDataHeaderMap.size() < MIN_NUM_FIELDS) {
      throw new LinkjaException(REQUIRED_COLUMNS_EXCEPTION_MESSAGE);
    }

    if (!this.patientDataHeaderMap.containsCanonicalColumn(PATIENT_ID_FIELD)
      || !this.patientDataHeaderMap.containsCanonicalColumn(FIRST_NAME_FIELD)
      || !this.patientDataHeaderMap.containsCanonicalColumn(LAST_NAME_FIELD)
      || !this.patientDataHeaderMap.containsCanonicalColumn(DATE_OF_BIRTH_FIELD)) {
      throw new LinkjaException(REQUIRED_COLUMNS_EXCEPTION_MESSAGE);
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
