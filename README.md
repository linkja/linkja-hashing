# linkja-hashing

The data linkage application is designed to disambiguate patients across sites / health systems, with minimal risk from transferring PHI by hashing the patient identifiers with a secured salt and hashing algorithm. Main Components of the application are:
1.	Web based Project, User and Key Manager
2.	Data Standardization, exceptions, and hashing 
3.	Disambiguation / matching

Each component is explained in more detail below:
1.	Web interface for Project, User and Key management - 
This component is designed to manage project specific Salt distribution. It allows the project managers to create projects, add users and authenticate the users. The authenticated users upload their RSA public keys and download the encrypted Salt file. 

2.	Data Standardization, exceptions, and hashing - 
This component includes a data pipeline to digest and validate the data, standardize the data, manage data exceptions, create composite variables from patient identifiers and hash using SHA512 algorithm and Salt

3.	Disambiguation / matching - 
This component allows the aggregator to merge files, disambiguate the hashes and assign Universal patient ID to matching patients


## Building
linkja-hashing was built using Java JDK 10 (specifically [OpenJDK](https://openjdk.java.net/)).  It can be opened from within an IDE like Eclipse or IntelliJ IDEA and compiled, or compiled from the command line using [Maven](https://maven.apache.org/).

You can build linkja-hashing via Maven:

`mvn clean package`

This will compile the code, run all unit tests, and create an executable JAR file under the .\target folder with all dependency JARs included.  The JAR will be named something like `Hashing-1.0-jar-with-dependencies.jar`.

## Program Use
You can run the executable JAR file using the standard Java command:
`java -jar Hashing-1.0-jar-with-dependencies.jar `

By default, the program will perform the hashing operations on an input file.  More information about the parameters needed to run hashing is shown below.

If you specify `--version`, the program will display the application version and the signature of the linkja-crypto library that it is using.  

Note that where files are used for input, they can be specified as a relative or absolute path.

### Hashing
Usage: `java -jar Hashing-1.0-jar-with-dependencies.jar --hashing`

The program is expecting a minimum of four parameters:

```
 -date,--privateDate <arg>         The private date (as MM/DD/YYYY)
 -key,--encryptionKey <arg>        Path to the aggregator's public key file, to
                                   encrypt results.
 -patient,--patientFile <arg>      Path to the file containing patient data
 -salt,--saltFile <arg>            Path to the salt file
```

There are additional optional parameters that you may also specify:

```
 -out,--outDirectory <arg>         The base directory to create output. If not
                                   specified, will use the current directory.
 -delim,--delimiter <arg>          The delimiter used within the patient data
                                   file. Uses a comma "," by default.
 --unhashed                        Write out the original unhashed data in the 
                                   result file (for debugging). false by default.
 --unencrypted                     Write out the result file as unencrypted text
                                   (for debugging). false by default.
```

**Examples:**

Required parameters specified

```
java -jar Hashing-1.0-jar-with-dependencies.jar --hashing
    --publicKey ./keys/public.key --saltFile ./data/project1_salt.txt
    --patientFile ./data/project1_patients.csv --privateDate 01/01/2018
```

Pipe delimited file as input, and specify the output.  Also includes writing out the unhashed data as unencrypted text for debugging.

```
java -jar Hashing-1.0-jar-with-dependencies.jar --hashing
    --publicKey ./keys/public.key --saltFile ./data/project1_salt.txt
    --patientFile ./data/project1_patients.csv --privateDate 01/01/2018
    --outDirectory ./data/output/ --delimiter | --unhashed --unencrypted
```
