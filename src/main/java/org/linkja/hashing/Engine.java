package org.linkja.hashing;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.linkja.hashing.steps.*;
import org.linkja.core.*;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Engine {
  public static final int MIN_NUM_FIELDS = 4;

  // Global rules that may be needed across multiple processing steps
  public static final int MIN_NAME_LENGTH = 2;

  // List the canonical names for anticipated fields
  public static final String PATIENT_ID_FIELD = "patient_id";
  public static final String FIRST_NAME_FIELD = "first_name";
  public static final String LAST_NAME_FIELD = "last_name";
  public static final String DATE_OF_BIRTH_FIELD = "date_of_birth";
  public static final String SOCIAL_SECURITY_NUMBER = "social_security_number";
  public static final String EXCLUSION = "exclusion";

  // List other canonical field names, used for output
  public static final String SITE_ID_FIELD = "siteid";
  public static final String PROJECT_ID_FIELD = "projectid";

  // Special fields used for different types of output files
  public static final String INVALID_DATA_ROW_NUMBER_FIELD = "row_number";
  public static final String INVALID_DATA_ERROR_DESCRIPTION_FIELD = "error_description";

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
  private CryptoHelper cryptoHelper = new CryptoHelper();

  private int numSubmittedJobs = 0;
  private int numCompletedJobs = 0;
  private int numInputRows = 0;
  private int numInvalidRows = 0;
  private int numDerivedRows = 0;

  private ArrayList<String> executionReport = new ArrayList<String>();

  private DateTimeFormatter fileTimestampFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");


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
    if (this.canonicalHeaderNames == null) {
      this.canonicalHeaderNames = new HashMap<String, String>();
      try (InputStream csvStream = getClass().getResourceAsStream("/configuration/canonical-header-names.csv")) {
        CSVParser parser = CSVParser.parse(csvStream, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
        for (CSVRecord csvRecord : parser) {
          this.canonicalHeaderNames.put(csvRecord.get(0).trim().toLowerCase(), csvRecord.get(1).trim().toLowerCase());
        }
      }
    }

    if (this.prefixes == null) {
      this.prefixes = new ArrayList<String>();
      try (BufferedReader buffer = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/configuration/prefixes.txt")))) {
        this.prefixes.addAll(buffer.lines().collect(Collectors.toList()));
      }
    }

    if (this.suffixes == null) {
      this.suffixes = new ArrayList<String>();
      try (BufferedReader buffer = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/configuration/suffixes.txt")))) {
        this.suffixes.addAll(buffer.lines().collect(Collectors.toList()));
      }
    }

    if (this.genericNames == null) {
      this.genericNames = new HashMap<String, String>();
      try (InputStream csvStream = getClass().getResourceAsStream("/configuration/generic-names.csv")) {
        CSVParser parser = CSVParser.parse(csvStream, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
        for (CSVRecord csvRecord : parser) {
          String name = csvRecord.get(0);
          String match = csvRecord.get(1);
          ExclusionStep.addMatchRuleToCollection(name, match, csvRecord.getRecordNumber(), this.genericNames);
        }
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
      row.put(entry.getCanonicalName() == null ? entry.getOriginalName() : entry.getCanonicalName(),
              csvRecord.get(entry.getOriginalName()));
    }
    row.setRowNumber(csvRecord.getRecordNumber());
    return row;
  }

  /**
   * Helper method to attempt closing all of our file writers
   * @param hashWriter
   * @param crosswalkWriter
   * @param invalidDataWriter
   * @throws IOException
   */
  private void closeWriters(BufferedWriter hashWriter, BufferedWriter crosswalkWriter, BufferedWriter invalidDataWriter, BufferedWriter combinedHashedUnhashedWriter) throws IOException {
    if (hashWriter != null) {
      hashWriter.close();
    }

    if (crosswalkWriter != null) {
      crosswalkWriter.close();
    }

    if (invalidDataWriter != null) {
      invalidDataWriter.close();
    }

    if (combinedHashedUnhashedWriter != null) {
      combinedHashedUnhashedWriter.close();
    }
  }

  /**
   * Run the hashing pipeline, given the current configuration
   * @throws IOException
   * @throws URISyntaxException
   * @throws LinkjaException
   */
  public void run() throws IOException, URISyntaxException, LinkjaException, InterruptedException {
    initialize();

    try (BufferedReader csvReader = new BufferedReader(new FileReader(parameters.getPatientFile()))) {
      CSVParser parser = CSVParser.parse(csvReader, CSVFormat.DEFAULT.withHeader().withDelimiter(this.parameters.getDelimiter()));
      Map<String, Integer> csvHeaderMap = parser.getHeaderMap();
      this.patientDataHeaderMap.createFromCSVHeader(csvHeaderMap).mergeCanonicalHeaders(this.canonicalHeaderNames);
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

      // Open up our output streams for hash results, crosswalks and invalid results.  We may have an optional 4th file
      // for a file mixing hashes and PHI.
      String fileTimestamp = LocalDateTime.now().format(fileTimestampFormatter);
      Path hashPath = Paths.get(this.parameters.getOutputDirectory().toString(),
              String.format("hashes_%s_%s_%s.csv", hashParameters.getSiteId(), hashParameters.getProjectId(), fileTimestamp));
      BufferedWriter hashWriter = Files.newBufferedWriter(hashPath);
      CSVPrinter hashPrinter = createHashPrinter(hashWriter);

      Path crosswalkPath = Paths.get(this.parameters.getOutputDirectory().toString(),
              String.format("DONOTSEND_crosswalk_%s_%s_%s.csv", hashParameters.getSiteId(), hashParameters.getProjectId(), fileTimestamp));
      BufferedWriter crosswalkWriter = Files.newBufferedWriter(crosswalkPath);
      CSVPrinter crosswalkPrinter = createCrosswalkPrinter(crosswalkWriter);

      Path invalidDataPath = Paths.get(this.parameters.getOutputDirectory().toString(),
              String.format("DONOTSEND_invaliddata_%s_%s_%s.csv", hashParameters.getSiteId(), hashParameters.getProjectId(), fileTimestamp));
      BufferedWriter invalidDataWriter = Files.newBufferedWriter(invalidDataPath);
      CSVPrinter invalidDataPrinter = createInvalidDataPrinter(invalidDataWriter);

      Path combinedHashedUnhashedPath = null;
      BufferedWriter combinedHashedUnhashedWriter = null;
      CSVPrinter combinedHashedUnhashedPrinter = null;

      if (parameters.isWriteUnhashedData()) {
        combinedHashedUnhashedPath = Paths.get(this.parameters.getOutputDirectory().toString(),
                String.format("DONOTSEND_reviewonly_%s_%s_%s.csv", hashParameters.getSiteId(), hashParameters.getProjectId(), fileTimestamp));
        combinedHashedUnhashedWriter = Files.newBufferedWriter(combinedHashedUnhashedPath);
        combinedHashedUnhashedPrinter = createCombinedHashedUnhashedPrinter(combinedHashedUnhashedWriter);
      }

      Path encryptedHashKeyPath = null;
      Path encryptedHashDataPath = null;
      if (parameters.isEncryptingOutput()) {
        encryptedHashKeyPath = Paths.get(this.parameters.getOutputDirectory().toString(),
                String.format("encHashKeyFile_%s_%s_%s.txt", hashParameters.getSiteId(), hashParameters.getProjectId(), fileTimestamp));
        encryptedHashDataPath = Paths.get(this.parameters.getOutputDirectory().toString(),
                String.format("enc_hashes_%s_%s_%s.csv", hashParameters.getSiteId(), hashParameters.getProjectId(), fileTimestamp));
      }

      Path[] allFilePaths = new Path[] { hashPath, crosswalkPath, invalidDataPath, combinedHashedUnhashedPath, encryptedHashKeyPath };

      // Reset all of our tracking counters right before we begin the processing cycle
      this.numSubmittedJobs = 0;
      this.numCompletedJobs = 0;
      this.numInvalidRows = 0;
      this.numDerivedRows = 0;
      this.numInputRows = 0;

      int batchSize = this.parameters.getBatchSize();
      int numThreads = this.parameters.getNumWorkerThreads();
      ArrayList<DataRow> batch = new ArrayList<DataRow>(batchSize);  // Pre-allocate the memory
      for (CSVRecord csvRecord : parser) {
        String patientId = csvRecord.get(patientIdIndex).trim();
        // If the patient ID is empty, we're not going to track it (that'd be a false error if there are multiple
        // blank fields on different rows).  Note that we're not throwing out the record as invalid at this point.  We
        // probably could, but we're delegating all of that work to our worker steps.
        if (!patientId.equals("")) {
          if (uniquePatientIds.contains(patientId)) {
            closeWriters(hashWriter, crosswalkWriter, invalidDataWriter, combinedHashedUnhashedWriter);
            deleteOutputFiles(allFilePaths);
            threadPool.shutdownNow();
            throw new LinkjaException(String.format("Patient IDs must be unique within the data file.  A duplicate copy of Patient ID %s was found on row %d.",
                    patientId.trim(), csvRecord.getRecordNumber()));
          }
          uniquePatientIds.add(patientId);
        }

        // We want to get our CSV data into a format that we can better act on within the application.  Load it to the
        // data structure we created for this work.
        DataRow row = csvRecordToDataRow(csvRecord);
        batch.add(row);
        this.numInputRows++;

        // Once we have a batch of work to do, create a new processing task.
        if (batch.size() == batchSize) {
          batch.trimToSize();  // Free up unused space
          taskQueue.submit(new EngineWorkerThread(batch, this.parameters.isRunNormalizationStep(),
                  this.parameters.getRecordExclusionMode(), this.prefixes, this.suffixes, this.genericNames,
                  this.hashParameters));
          batch.clear();  // The worker thread has its copy, so we can clear ours out to start a new batch
          this.numSubmittedJobs++;

          System.out.printf("Loaded %d records for hashing\r\n", csvRecord.getRecordNumber());

          // As we submit a new batch to be worked on, we will look to see if we're exceeding the number of threads we
          // wanted to have running.  If so, we'll start clearing up results (if they are done) and writing them up, which
          // will keep memory usage manageable.
          while (((ThreadPoolExecutor) threadPool).getActiveCount() >= numThreads) {
            if (!processPendingWork(taskQueue, hashPrinter, crosswalkPrinter, invalidDataPrinter, combinedHashedUnhashedPrinter, false)) {
              // If there was a problem during the middle of the processing cycle, we have to stop work since we can't
              // rely on our results.
              threadPool.shutdownNow();
              closeWriters(hashWriter, crosswalkWriter, invalidDataWriter, combinedHashedUnhashedWriter);
              deleteOutputFiles(allFilePaths);
              return;
            }
          }
        }
      }

      // If we have a partial batch that was filled, make sure it gets submitted to the work queue
      if (batch.size() > 0) {
        batch.trimToSize();
        taskQueue.submit(new EngineWorkerThread(batch, this.parameters.isRunNormalizationStep(),
                this.parameters.getRecordExclusionMode(), this.prefixes, this.suffixes, this.genericNames,
                this.hashParameters));
        this.numSubmittedJobs++;
      }

      boolean finalProcessSucceeded = processPendingWork(taskQueue, hashPrinter, crosswalkPrinter, invalidDataPrinter, combinedHashedUnhashedPrinter, true);
      closeWriters(hashWriter, crosswalkWriter, invalidDataWriter, combinedHashedUnhashedWriter);

      if (finalProcessSucceeded) {
        executionReport.add("Completed processing results:");
        executionReport.add(String.format("  %d data rows read", this.numInputRows));
        int totalHashedRows = (this.numInputRows - this.numInvalidRows + this.numDerivedRows);
        executionReport.add(String.format("  %d total hashed rows created", totalHashedRows));
        executionReport.add(String.format("     %d original data rows hashed", (totalHashedRows - this.numDerivedRows)));
        executionReport.add(String.format("     %d derived rows hashed", this.numDerivedRows));
        executionReport.add(String.format("  %d invalid rows", this.numInvalidRows));

        // If we are encrypting the hashed results, do that now
        if (parameters.isEncryptingOutput()) {
          try {
            System.out.println("Preparing to encrypt hash file...");
            CryptoHelper.AESParameters aesParameters = cryptoHelper.generateAESParameters();
            cryptoHelper.encryptAES(aesParameters, hashPath.toFile(), encryptedHashDataPath.toFile());
            cryptoHelper.rsaEncryptAES(aesParameters, encryptedHashKeyPath.toFile(), parameters.getEncryptionKeyFile());
            System.out.println("Hash file encrypted");
          } catch (Exception e) {
            throw new LinkjaException("There was an error when trying to encrypt the hashed output.  The unencrypted hash files have been preserved.");
          }
        }
      }
      else {
        deleteOutputFiles(allFilePaths);
      }

      threadPool.shutdown();

      // Ensure at this point (as a final sanity check) that all of our submitted jobs have been tracked to completion
      if (this.numCompletedJobs != this.numSubmittedJobs) {
        throw new LinkjaException(String.format("We are missing %d jobs - expected %d, tracked %d to completion.\r\nThe unencrypted hash files have been preserved, but are incomplete.",
                (this.numSubmittedJobs - this.numCompletedJobs), this.numSubmittedJobs, this.numCompletedJobs));
      }

    }
  }

  /**
   * Delete all output files that were established.  This is a helper method, so it assumes we know the files are there
   * and can be deleted.
   * @param paths
   * @throws IOException
   */
  private void deleteOutputFiles(Path[] paths) throws IOException {
    if (paths == null || paths.length == 0) {
      return;
    }

    for(Path path : paths) {
      if (path != null) {
        Files.delete(path);
      }
    }
  }

  /**
   * Check our queue of submitted work tasks and process the completed rows (writing them to our putput CSV).
   * @param taskQueue
   * @param hashPrinter
   * @param crosswalkPrinter
   * @param invalidDataPrinter
   * @param waitUntilComplete
   * @return true if the work was processed, false if there was an error
   */
  private boolean processPendingWork(ExecutorCompletionService<List<DataRow>> taskQueue, CSVPrinter hashPrinter,
                                     CSVPrinter crosswalkPrinter, CSVPrinter invalidDataPrinter, CSVPrinter combinedHashedUnhashedPrinter,
                                     boolean waitUntilComplete) {
    try {
      // Go through at least once to check the status of our jobs and process pending results.  If the flag has been
      // set to wait until everything is completed, we will stay in a loop checking for completed work.
      do {
        // If we have completed all of the submitted jobs, we are all done.  If we have some outstanding, we are going to
        // set a polling timeout.
        if (this.numCompletedJobs == this.numSubmittedJobs) {
          System.out.println("All jobs have been processed");
          return true;
        }

        Future<List<DataRow>> task = taskQueue.poll(500, TimeUnit.MILLISECONDS);
        while (task != null) {
          writeDataRowResults(task.get(), hashPrinter, crosswalkPrinter, invalidDataPrinter, combinedHashedUnhashedPrinter);
          this.numCompletedJobs++;
          task = taskQueue.poll(500, TimeUnit.MILLISECONDS);
        }

        // We will sleep briefly just so we're not churning waiting for work to finish.
        if (waitUntilComplete) {
          System.out.println("Waiting for all working threads to complete...");
          Thread.sleep(1000);
        }
      } while (waitUntilComplete);
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
  private CSVPrinter createHashPrinter(BufferedWriter writer) throws IOException {
    return new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
            Engine.SITE_ID_FIELD,
            Engine.PROJECT_ID_FIELD,
            HashingStep.PIDHASH_FIELD,
            // We need to generalize the labels for these hashes, so they don't disclose to an attacker additional
            // information about their composition.  Comments show the actual field each is mapped to.
            "hash1",  // HashingStep.FNAMELNAMEDOBSSN_FIELD
            "hash2",  // HashingStep.FNAMELNAMEDOBSSN_FIELD
            "hash3",  // HashingStep.LNAMEFNAMEDOBSSN_FIELD
            "hash4",  // HashingStep.LNAMEFNAMEDOB_FIELD
            "hash5",  // HashingStep.FNAMELNAMETDOBSSN_FIELD
            "hash6",  // HashingStep.FNAMELNAMETDOB_FIELD
            "hash7",  // HashingStep.FNAME3LNAMEDOBSSN_FIELD
            "hash8",  // HashingStep.FNAME3LNAMEDOB_FIELD
            "hash9",  // HashingStep.FNAMELNAMEDOBDSSN_FIELD
            "hash10", // HashingStep.FNAMELNAMEDOBYSSN_FIELD
            Engine.EXCLUSION));
  }

  private CSVPrinter createCombinedHashedUnhashedPrinter(BufferedWriter writer) throws IOException {
    return new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
            Engine.SITE_ID_FIELD,
            Engine.PROJECT_ID_FIELD,
            Engine.PATIENT_ID_FIELD,
            Engine.FIRST_NAME_FIELD,
            Engine.LAST_NAME_FIELD,
            Engine.DATE_OF_BIRTH_FIELD,
            Engine.SOCIAL_SECURITY_NUMBER,
            HashingStep.PIDHASH_FIELD,
            // We need to generalize the labels for these hashes, so they don't disclose to an attacker additional
            // information about their composition.  Comments show the actual field each is mapped to.
            "hash1",  // HashingStep.FNAMELNAMEDOBSSN_FIELD
            "hash2",  // HashingStep.FNAMELNAMEDOBSSN_FIELD
            "hash3",  // HashingStep.LNAMEFNAMEDOBSSN_FIELD
            "hash4",  // HashingStep.LNAMEFNAMEDOB_FIELD
            "hash5",  // HashingStep.FNAMELNAMETDOBSSN_FIELD
            "hash6",  // HashingStep.FNAMELNAMETDOB_FIELD
            "hash7",  // HashingStep.FNAME3LNAMEDOBSSN_FIELD
            "hash8",  // HashingStep.FNAME3LNAMEDOB_FIELD
            "hash9",  // HashingStep.FNAMELNAMEDOBDSSN_FIELD
            "hash10", // HashingStep.FNAMELNAMEDOBYSSN_FIELD
            Engine.EXCLUSION));
  }

  /**
   * Creates a CSVPrinter object to be used for creating our crosswalk data
   * @param writer
   * @return
   * @throws IOException
   */
  private CSVPrinter createCrosswalkPrinter(BufferedWriter writer) throws IOException {
    return new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
            Engine.PATIENT_ID_FIELD,
            HashingStep.PIDHASH_FIELD));
  }

  /**
   * Creates a CSVPrinter object to be used for creating our invalid data file
   * @param writer
   * @return
   * @throws IOException
   */
  private CSVPrinter createInvalidDataPrinter(BufferedWriter writer) throws IOException {
      return new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
              Engine.INVALID_DATA_ROW_NUMBER_FIELD,
              Engine.PATIENT_ID_FIELD,
              Engine.FIRST_NAME_FIELD,
              Engine.LAST_NAME_FIELD,
              Engine.DATE_OF_BIRTH_FIELD,
              Engine.SOCIAL_SECURITY_NUMBER,
              Engine.INVALID_DATA_ERROR_DESCRIPTION_FIELD));
  }

  /**
   * Write out a collection of DataRow results
   * @param hashPrinter
   * @param crosswalkPrinter
   * @param invalidDataPrinter
   * @param rows
   * @throws IOException
   */
  private void writeDataRowResults(List<DataRow> rows, CSVPrinter hashPrinter,
                                   CSVPrinter crosswalkPrinter, CSVPrinter invalidDataPrinter, CSVPrinter combinedHashedUnhashedPrinter) throws IOException {
    if (rows == null) {
      return;
    }

    for (DataRow row : rows) {
      writeDataRowResult(row, hashPrinter, crosswalkPrinter, invalidDataPrinter, combinedHashedUnhashedPrinter);
    }
  }

  /**
   * Utility method to write out the appropriate columns for a data row.
   * @param hashPrinter
   * @param crosswalkPrinter
   * @param invalidDataPrinter
   * @param row
   * @throws IOException
   */
  private void writeDataRowResult(DataRow row, CSVPrinter hashPrinter,
                                  CSVPrinter crosswalkPrinter, CSVPrinter invalidDataPrinter, CSVPrinter combinedHashedUnhashedPrinter) throws IOException {
    if (row == null) {
      return;
    }

    // If the SSN is not set, we write out a blank string.  Although we can generate a hash with a blank SSN, it's
    // potentially misleading during the matching phase, so we choose instead to not provide output for SSN-related hashes.
    boolean hasSsn = !(row.get(Engine.SOCIAL_SECURITY_NUMBER) == null || row.get(Engine.SOCIAL_SECURITY_NUMBER).equals(""));
    if (row.shouldProcess()) {
      crosswalkPrinter.printRecord(row.get(Engine.PATIENT_ID_FIELD), row.get(HashingStep.PIDHASH_FIELD));

      hashPrinter.printRecord(
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

      if (parameters.isWriteUnhashedData()) {
        combinedHashedUnhashedPrinter.printRecord(
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

      // Process all derived rows as well
      if (row.hasDerivedRows()) {
        for (DataRow derivedRow : row.getDerivedRows()) {
          this.numDerivedRows++;
          writeDataRowResult(derivedRow, hashPrinter, crosswalkPrinter, invalidDataPrinter, combinedHashedUnhashedPrinter);
        }
      }
    }
    else {
      this.numInvalidRows++;
      invalidDataPrinter.printRecord(
              row.getRowNumber(),
              row.get(Engine.PATIENT_ID_FIELD),
              row.get(Engine.FIRST_NAME_FIELD),
              row.get(Engine.LAST_NAME_FIELD),
              row.get(Engine.DATE_OF_BIRTH_FIELD),
              hasSsn ? row.get(Engine.SOCIAL_SECURITY_NUMBER) : "",
              row.getInvalidReason().replaceAll("\r\n", "|"));
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

  public EngineParameters getParameters() {
    return parameters;
  }

  public ArrayList<String> getExecutionReport() {
    return executionReport;
  }
}
