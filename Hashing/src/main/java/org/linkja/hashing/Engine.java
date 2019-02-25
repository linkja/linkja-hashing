package org.linkja.hashing;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.linkja.hashing.steps.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class Engine {
  public static final int MIN_NUM_FIELDS = 4;

  // List the canonical names for anticipated fields
  public static final String PATIENT_ID_FIELD = "patient_id";
  public static final String FIRST_NAME_FIELD = "first_name";
  public static final String LAST_NAME_FIELD = "last_name";
  public static final String DATE_OF_BIRTH_FIELD = "date_of_birth";
  public static final String SOCIAL_SECURITY_NUMBER = "social_security_number";
  public static final String EXCEPTION_FLAG = "exception_flag";

  private static final String REQUIRED_COLUMNS_EXCEPTION_MESSAGE = "We require, at a minimum, columns for patient ID, first name, last name, and date of birth.\r\n" +
          "If you have these columns present, please make sure that the column header is specified, and that the column names are in our list of " +
          "recognized values.  Please see the project README for more information.";

  private EngineParameters parameters;
  private static HashParameters hashParameters;
  private DataHeaderMap patientDataHeaderMap;
  private static HashMap<String, String> canonicalHeaderNames = null;
  private static ArrayList<String> prefixes = null;
  private static ArrayList<String> suffixes = null;
  private static HashMap<String, String> genericNames = null;


  public Engine(EngineParameters parameters, HashParameters hashParameters) {
    this.parameters = parameters;
    this.hashParameters = hashParameters;
    this.patientDataHeaderMap = new DataHeaderMap();
  }

  /**
   * Performs initialization and setup for the Engine.  This assumes that it has been given all of the parameters.
   * This should be called if any parameters change.
   * @throws IOException
   * @throws URISyntaxException
   * @throws LinkjaException
   */
  public void initialize() throws IOException, URISyntaxException, LinkjaException {
    ClassLoader classLoader = getClass().getClassLoader();
    if (this.canonicalHeaderNames == null) {
      canonicalHeaderNames = new HashMap<String, String>();
      File file = new File(classLoader.getResource("configuration/canonical-header-names.csv").getFile());
      CSVParser parser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
      for (CSVRecord csvRecord : parser) {
        canonicalHeaderNames.put(csvRecord.get(0), csvRecord.get(1));
      }
    }

    if (this.prefixes == null) {
      this.prefixes = new ArrayList<String>();
      Path path = Paths.get(classLoader.getResource("configuration/prefixes.txt").toURI());
      prefixes.addAll(Files.readAllLines(path));
    }

    if (this.suffixes == null) {
      this.suffixes = new ArrayList<String>();
      Path path = Paths.get(classLoader.getResource("configuration/suffixes.txt").toURI());
      suffixes.addAll(Files.readAllLines(path));
    }

    if (this.genericNames == null) {
      this.genericNames = new HashMap<String, String>();
      File file = new File(classLoader.getResource("configuration/generic-names.csv").getFile());
      CSVParser parser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
      for (CSVRecord csvRecord : parser) {
        String name = csvRecord.get(0);
        String match = csvRecord.get(1);
        ExceptionStep.addMatchRuleToCollection(name, match, csvRecord.getRecordNumber(), this.genericNames);
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

  private static class CalculationThread implements Callable<DataRow> {
    private DataRow dataRow;
    private boolean runNormalizationStep;
    private EngineParameters.RecordExceptionMode exceptionMode;
    private static ArrayList<String> prefixes = null;
    private static ArrayList<String> suffixes = null;
    private static HashMap<String, String> genericNames = null;
    private static HashParameters hashParameters;

    public CalculationThread(DataRow dataRow, boolean runNormalizationStep,EngineParameters.RecordExceptionMode exceptionMode,
                             ArrayList<String> prefixes, ArrayList<String> suffixes, HashMap<String, String> genericNames,
                             HashParameters hashParameters) {
      this.dataRow = dataRow;
      this.runNormalizationStep = runNormalizationStep;
      this.exceptionMode = exceptionMode;
      this.prefixes = prefixes;
      this.suffixes = suffixes;
      this.genericNames = genericNames;
      this.hashParameters = hashParameters;
    }

    @Override
    public DataRow call() throws Exception {
      ArrayList<IStep> steps = new ArrayList<IStep>();
      steps.add(new ValidationFilterStep());
      // The user can configure the system to skip normalization, if they prefer to handle that externally
      if (runNormalizationStep) {
        steps.add(new NormalizationStep(this.prefixes, this.suffixes));
      }
      // If the user doesn't want exception flags, or has already created them, we are going to skip this step in
      // the processing pipeline.
      // TODO - If the user said they have already provided the exception flag, should we confirm that?
      if (exceptionMode == EngineParameters.RecordExceptionMode.GenerateExceptions) {
        steps.add(new ExceptionStep(this.genericNames));
      }
      steps.add(new PermuteStep());
      steps.add(new HashingStep(this.hashParameters));

      RecordProcessor processor = new RecordProcessor(steps);
      return processor.run(this.dataRow);
    }
  }

  /**
   * Run the hashing pipeline, given the current configuration
   * @throws IOException
   * @throws URISyntaxException
   * @throws LinkjaException
   */
  public void run() throws IOException, URISyntaxException, LinkjaException {
    initialize();

    CSVParser parser = CSVParser.parse(parameters.getPatientFile(), Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader().withDelimiter(this.parameters.getDelimiter()));
    Map<String, Integer> csvHeaderMap = parser.getHeaderMap();
    this.patientDataHeaderMap.createFromCSVHeader(csvHeaderMap).mergeCanonicalHeaders(normalizeHeader(csvHeaderMap));
    verifyFields();

    HashSet<String> uniquePatientIds = new HashSet<>();
    int patientIdIndex = this.patientDataHeaderMap.findIndexOfCanonicalName(PATIENT_ID_FIELD);

    ExecutorService threadPool = Executors.newFixedThreadPool(parameters.getNumWorkerThreads());
    ExecutorCompletionService<DataRow> taskQueue = new ExecutorCompletionService<DataRow>(threadPool);

    // Because our check for unique patient IDs requires knowing every ID that has been seen, and because our processing
    // steps do not hold state, we are performing this check as we load up our worker queue.
    int numSubmittedJobs = 0;
    for (CSVRecord csvRecord : parser) {
      String patientId = csvRecord.get(patientIdIndex);
      if (uniquePatientIds.contains(patientId)) {
        threadPool.shutdownNow();
        throw new LinkjaException(String.format("Patient IDs must be unique within the data file.  A duplicate copy of Patient ID %s was found on row %ld.",
                patientId, csvRecord.getRecordNumber()));
      }
      uniquePatientIds.add(patientId);

      DataRow row = csvRecordToDataRow(csvRecord);
      Future<DataRow> result = taskQueue.submit(new CalculationThread(row, this.parameters.isRunNormalizationStep(),
              this.parameters.getRecordExceptionMode(), this.prefixes, this.suffixes, this.genericNames,
              this.hashParameters));
      numSubmittedJobs++;
    }

    //Output the long values of all my tasks in order of completion.
    try {
      for(int index = 0; index < numSubmittedJobs; index++) {
        Future<DataRow> task = taskQueue.take();
        writeDataRowResult(task.get());
      }
    }
    catch (Exception exc) {
      exc.printStackTrace();
    }

    threadPool.shutdown();
  }

  private void writeDataRowResult(DataRow row) {
    if (row == null) {
      return;
    }

    if (row.getInvalidReason() == null || row.getInvalidReason().equals("")) {

    }
    else {
      System.out.println("INVALID ROW: ");
      dumpRow(row);
    }
  }

  /**
   * Utility method for development purposes only.  This writes a given data row
   * to the console.
   * @param row The DataRow to write out
   */
  private void dumpRow(DataRow row) {
    dumpRow(row, 0);
  }

  /**
   * Utility method for development purposes only.  This writes a given data row
   * to the console.
   * @param row The DataRow to write out
   * @param indentLevel The number of spaces to indent output (used for nested derived rows)
   */
  private void dumpRow(DataRow row, int indentLevel) {
    String indentString = new String(new char[indentLevel*2]).replace("\0", " ");
    for (Map.Entry<String,String> entry : row.entrySet()) {
      System.out.printf("%s%s - %s\r\n", indentString, entry.getKey(), entry.getValue());
    }
    if (row.getInvalidReason() != null && !row.getInvalidReason().equals("")) {
      System.out.printf("%sINVALID REASON: - %s\r\n", indentString, row.getInvalidReason());
    }
    if (row.hasDerivedRows()) {
      System.out.printf("%sDERIVED ROWS:\r\n", indentString);
      for (DataRow derivedRow : row.getDerivedRows()) {
        dumpRow(derivedRow, indentLevel+1);
      }
    }
    System.out.println();
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
}
