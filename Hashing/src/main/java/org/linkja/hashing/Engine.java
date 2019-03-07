package org.linkja.hashing;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.linkja.hashing.steps.*;

import java.io.BufferedWriter;
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

  // List other canonical field names, used for output
  public static final String SITE_ID_FIELD = "siteid";
  public static final String PROJECT_ID_FIELD = "projectid";

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

  private int numSubmittedJobs = 0;
  private int numCompletedJobs = 0;

  private ArrayList<String> executionReport = new ArrayList<String>();


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

  /**
   * Run the hashing pipeline, given the current configuration
   * @throws IOException
   * @throws URISyntaxException
   * @throws LinkjaException
   */
  public void run() throws IOException, URISyntaxException, LinkjaException, InterruptedException {
    initialize();

    CSVParser parser = CSVParser.parse(parameters.getPatientFile(), Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader().withDelimiter(this.parameters.getDelimiter()));
    Map<String, Integer> csvHeaderMap = parser.getHeaderMap();
    this.patientDataHeaderMap.createFromCSVHeader(csvHeaderMap).mergeCanonicalHeaders(normalizeHeader(csvHeaderMap));
    verifyFields();

    // Because our check for unique patient IDs requires knowing every ID that has been seen, and because our processing
    // steps do not hold state, we are performing this check as we load up our worker queue.
    HashSet<String> uniquePatientIds = new HashSet<>();
    int patientIdIndex = this.patientDataHeaderMap.findIndexOfCanonicalName(PATIENT_ID_FIELD);

    // Build our thread pool and task queue - these are configured with a set number of threads (that the user can define),
    // and will run such that that number of threads is the total amount of work we have queued up.
    ExecutorService threadPool = new ThreadPoolExecutor(parameters.getNumWorkerThreads(), parameters.getNumWorkerThreads(),30,TimeUnit.MINUTES,
            new ArrayBlockingQueue<Runnable>(parameters.getNumWorkerThreads()), new ThreadPoolExecutor.CallerRunsPolicy());
    ExecutorCompletionService<List<DataRow>> taskQueue = new ExecutorCompletionService<List<DataRow>>(threadPool);

    // Open up our output stream
    BufferedWriter writer = Files.newBufferedWriter(Paths.get(this.parameters.getOutputDirectory().toString(),"output.csv"));
    CSVPrinter csvPrinter = createCSVPrinter(writer);

    this.numSubmittedJobs = 0;
    this.numCompletedJobs = 0;
    int totalRows = 0;
    int batchSize = this.parameters.getBatchSize();
    int numThreads = this.parameters.getNumWorkerThreads();
    ArrayList<DataRow> batch = new ArrayList<DataRow>(batchSize);  // Pre-allocate the memory
    for (CSVRecord csvRecord : parser) {
      String patientId = csvRecord.get(patientIdIndex);
      if (uniquePatientIds.contains(patientId)) {
        writer.close();
        threadPool.shutdownNow();
        throw new LinkjaException(String.format("Patient IDs must be unique within the data file.  A duplicate copy of Patient ID %s was found on row %d.",
                patientId.trim(), csvRecord.getRecordNumber()));
      }
      uniquePatientIds.add(patientId);

      // We want to get our CSV data into a format that we can better act on within the application.  Load it to the
      // data structure we created for this work.
      DataRow row = csvRecordToDataRow(csvRecord);
      batch.add(row);
      totalRows++;

      // Once we have a batch of work to do, create a new processing task.
      if (batch.size() == batchSize) {
        batch.trimToSize();  // Free up unused space
        taskQueue.submit(new EngineWorkerThread(batch, this.parameters.isRunNormalizationStep(),
                this.parameters.getRecordExceptionMode(), this.prefixes, this.suffixes, this.genericNames,
                this.hashParameters));
        batch.clear();  // The worker thread has its copy, so we can clear ours out to start a new batch
        this.numSubmittedJobs++;

        System.out.printf("Loaded %d records for hashing\r\n", csvRecord.getRecordNumber());

        // As we submit a new batch to be worked on, we will look to see if we're exceeding the number of threads we
        // wanted to have running.  If so, we'll start clearing up results (if they are done) and writing them up, which
        // will keep memory usage manageable.
        while (((ThreadPoolExecutor) threadPool).getActiveCount() >= numThreads) {
          if (!processPendingWork(taskQueue, csvPrinter)) {
            // If there was a problem during the middle of the processing cycle, we have to stop work since we can't
            // rely on our results.
            threadPool.shutdownNow();
            writer.close();
            return;
          }

          // We will sleep briefly just so we're not churning waiting for work to finish.
          Thread.sleep(500);
        }

      }
    }

    // If we have a partial batch that was filled, make sure it gets submitted to the work queue
    if (batch.size() > 0) {
      batch.trimToSize();
      taskQueue.submit(new EngineWorkerThread(batch, this.parameters.isRunNormalizationStep(),
              this.parameters.getRecordExceptionMode(), this.prefixes, this.suffixes, this.genericNames,
              this.hashParameters));
      this.numSubmittedJobs++;
    }

    if (processPendingWork(taskQueue, csvPrinter)) {
      executionReport.add(String.format("Completed hashing %d data rows", totalRows));
    }

    writer.close();
    threadPool.shutdown();
  }

  /**
   * Check our queue of submitted work tasks and process the completed rows (writing them to our putput CSV).
   * @param taskQueue
   * @param csvPrinter
   * @return true if the work was processed, false if there was an error
   */
  private boolean processPendingWork(ExecutorCompletionService<List<DataRow>> taskQueue, CSVPrinter csvPrinter) {
    try {
      // If we have completed all of the submitted jobs, we are all done.  If we have some outstanding, we are going to
      // set a polling timeout.
      if (this.numCompletedJobs == this.numSubmittedJobs) {
        System.out.println("All jobs have been processed");
        return true;
      }

      Future<List<DataRow>> task = taskQueue.poll(500, TimeUnit.MILLISECONDS);
      while (task != null) {
        writeDataRowResults(csvPrinter, task.get());
        this.numCompletedJobs++;
        task = taskQueue.poll(500, TimeUnit.MILLISECONDS);
      }
      csvPrinter.flush();
    }
    catch (Exception exc) {
      executionReport.add("An exception was thrown while writing out the results.  The output file is incomplete and should not be used.");
      executionReport.add(exc.getMessage());
      exc.printStackTrace();
      return false;
    }

    return true;
  }

  /**
   * Utility method to create the CSVPrinter object with the appropriate header columns (based on parameters on how
   * the Engine should run)
    * @param writer An open file writer to connect the CSVPrinter to
   * @return The instantiated CSVPrinter object
   * @throws IOException
   */
  private CSVPrinter createCSVPrinter(BufferedWriter writer) throws IOException {
    if (parameters.isWriteUnhashedData()) {
      return new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
              Engine.SITE_ID_FIELD,
              Engine.PROJECT_ID_FIELD,
              Engine.PATIENT_ID_FIELD,
              Engine.FIRST_NAME_FIELD,
              Engine.LAST_NAME_FIELD,
              Engine.DATE_OF_BIRTH_FIELD,
              Engine.SOCIAL_SECURITY_NUMBER,
              HashingStep.PIDHASH_FIELD,
              HashingStep.FNAMELNAMEDOBSSN_FIELD,
              HashingStep.FNAMELNAMEDOB_FIELD,
              HashingStep.LNAMEFNAMEDOBSSN_FIELD,
              HashingStep.LNAMEFNAMEDOB_FIELD,
              HashingStep.FNAMELNAMETDOBSSN_FIELD,
              HashingStep.FNAMELNAMETDOB_FIELD,
              HashingStep.FNAME3LNAMEDOBSSN_FIELD,
              HashingStep.FNAME3LNAMEDOB_FIELD,
              HashingStep.FNAMELNAMEDOBDSSN_FIELD,
              HashingStep.FNAMELNAMEDOBYSSN_FIELD,
              Engine.EXCEPTION_FLAG));
    }
    else {
      return new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
              Engine.SITE_ID_FIELD,
              Engine.PROJECT_ID_FIELD,
              HashingStep.PIDHASH_FIELD,
              HashingStep.FNAMELNAMEDOBSSN_FIELD,
              HashingStep.FNAMELNAMEDOB_FIELD,
              HashingStep.LNAMEFNAMEDOBSSN_FIELD,
              HashingStep.LNAMEFNAMEDOB_FIELD,
              HashingStep.FNAMELNAMETDOBSSN_FIELD,
              HashingStep.FNAMELNAMETDOB_FIELD,
              HashingStep.FNAME3LNAMEDOBSSN_FIELD,
              HashingStep.FNAME3LNAMEDOB_FIELD,
              HashingStep.FNAMELNAMEDOBDSSN_FIELD,
              HashingStep.FNAMELNAMEDOBYSSN_FIELD,
              Engine.EXCEPTION_FLAG));
    }
  }

  private void writeDataRowResults(CSVPrinter dataPrinter, List<DataRow> rows) throws IOException {
    if (rows == null) {
      return;
    }

    for (DataRow row : rows) {
      writeDataRowResult(dataPrinter, row);
    }
    dataPrinter.flush();
  }

  /**
   * Utility method to write out the appropriate columns for a data row.
   * @param dataPrinter
   * @param row
   * @throws IOException
   */
  private void writeDataRowResult(CSVPrinter dataPrinter, DataRow row) throws IOException {
    if (row == null) {
      return;
    }

    // If the SSN is not set, we write out a blank string.  Although we can generate a hash with a blank SSN, it's
    // potentially misleading during the matching phase, so we choose instead to not provide output for SSN-related hashes.
    boolean hasSsn = !(row.get(Engine.SOCIAL_SECURITY_NUMBER) == null || row.get(Engine.SOCIAL_SECURITY_NUMBER).equals(""));
    if (row.shouldProcess()) {
      if (parameters.isWriteUnhashedData()) {
        dataPrinter.printRecord(
                this.hashParameters.getSiteId(),
                this.hashParameters.getProjectId(),
                row.get(Engine.PATIENT_ID_FIELD),
                row.get(Engine.FIRST_NAME_FIELD),
                row.get(Engine.LAST_NAME_FIELD),
                row.get(Engine.DATE_OF_BIRTH_FIELD),
                hasSsn ? row.get(Engine.SOCIAL_SECURITY_NUMBER) : "",
                row.get(HashingStep.PIDHASH_FIELD),
                hasSsn ? row.get(HashingStep.FNAMELNAMEDOBSSN_FIELD) : "",
                row.get(HashingStep.FNAMELNAMEDOB_FIELD),
                hasSsn ? row.get(HashingStep.LNAMEFNAMEDOBSSN_FIELD) : "",
                row.get(HashingStep.LNAMEFNAMEDOB_FIELD),
                hasSsn ? row.get(HashingStep.FNAMELNAMETDOBSSN_FIELD) : "",
                row.get(HashingStep.FNAMELNAMETDOB_FIELD),
                hasSsn ? row.get(HashingStep.FNAME3LNAMEDOBSSN_FIELD) : "",
                row.get(HashingStep.FNAME3LNAMEDOB_FIELD),
                hasSsn ? row.get(HashingStep.FNAMELNAMEDOBDSSN_FIELD) : "",
                hasSsn ? row.get(HashingStep.FNAMELNAMEDOBYSSN_FIELD) : "",
                row.isException() ? "1" : "0"
        );
      }
      else {
        dataPrinter.printRecord(
                this.hashParameters.getSiteId(),
                this.hashParameters.getProjectId(),
                row.get(HashingStep.PIDHASH_FIELD),
                hasSsn ? row.get(HashingStep.FNAMELNAMEDOBSSN_FIELD) : "",
                row.get(HashingStep.FNAMELNAMEDOB_FIELD),
                hasSsn ? row.get(HashingStep.LNAMEFNAMEDOBSSN_FIELD) : "",
                row.get(HashingStep.LNAMEFNAMEDOB_FIELD),
                hasSsn ? row.get(HashingStep.FNAMELNAMETDOBSSN_FIELD) : "",
                row.get(HashingStep.FNAMELNAMETDOB_FIELD),
                hasSsn ? row.get(HashingStep.FNAME3LNAMEDOBSSN_FIELD) : "",
                row.get(HashingStep.FNAME3LNAMEDOB_FIELD),
                hasSsn ? row.get(HashingStep.FNAMELNAMEDOBDSSN_FIELD) : "",
                hasSsn ? row.get(HashingStep.FNAMELNAMEDOBYSSN_FIELD) : "",
                row.isException() ? "1" : "0"
        );
      }

      // Process all derived rows as well
      if (row.hasDerivedRows()) {
        for (DataRow derivedRow : row.getDerivedRows()) {
          writeDataRowResult(dataPrinter, derivedRow);
        }
      }
    }
    else {
      System.out.printf("INVALID ROW: row number %d\r\n", row.getRowNumber());
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

  public ArrayList<String> getExecutionReport() {
    return executionReport;
  }
}
