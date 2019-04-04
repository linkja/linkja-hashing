# linkja

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
linkja-hashing was built using Java JDK 1.8 (specifically [OpenJDK](https://openjdk.java.net/)).  It can be opened from within an IDE like Eclipse or IntelliJ IDEA and compiled, or compiled from the command line using [Maven](https://maven.apache.org/).

`mvn clean package`

This will compile the code, run all unit tests, and create an executable JAR file under the .\target folder with all dependency JARs included.  The JAR will be named something like `Hashing-1.0-jar-with-dependencies.jar`.

## Program Use
You can run the executable JAR file using the standard Java command:
`java -jar Hashing-1.0-jar-with-dependencies.jar `

The program has two modes that can be invoked - the primary one (enabled with `--hashing`) will perform the hashing operations on an input file.  The second mode (which can be used by itself or with hashing) is enabled with `--displaySalt`.  This will decrypt and display the contents of the salt file.  More information about the parameters needed for each of the modes is shown below.

Note that where files are used for input, they can be specified as a relative or absolute path.

### Hashing
Usage: `java -jar Hashing-1.0-jar-with-dependencies.jar --hashing`

The program is expecting a minimum of four parameters:

```
 -date,--privateDate <arg>         The private date (as MM/DD/YYYY)
 -key,--privateKey <arg>           Path to the private key file
 -patient,--patientFile <arg>      Path to the file containing patient data
 -salt,--saltFile <arg>            Path to encrypted salt file
```

There are additional optional parameters that you may also specify:

```
 -out,--outDirectory <arg>         The base directory to create output. If not
                                   specified, will use the current directory.
 -delim,--delimiter <arg>          The delimiter used within the patient data
                                   file. Uses a comma "," by default.
 -unhashed,--writeUnhashed         write out the original unhashed data in the 
                                   result file (for debugging). false by default.
```

**Examples:**

Required parameters specified

```
java -jar Hashing-1.0-jar-with-dependencies.jar --hashing
    --privateKey ./keys/private.key --saltFile ./data/project1_stalt_encrypted.txt
    --patientFile ./data/project1_patients.csv --privateDate 01/01/2018
```

Pipe delimited file as input, and specify the output.  Also includes writing out the unhashed data for debugging.

```
java -jar Hashing-1.0-jar-with-dependencies.jar --hashing
    --privateKey ./keys/private.key --saltFile ./data/project1_stalt_encrypted.txt
    --patientFile ./data/project1_patients.csv --privateDate 01/01/2018
    --outDirectory ./data/output/ --delimiter | --writeUnhashed
```


### Display Salt
Usage: `java -jar Hashing-1.0-jar-with-dependencies.jar --displaySalt`

The program is expecting a minimum of two parameters:

```
 -key,--privateKey <arg>           Path to the private key file
 -salt,--saltFile <arg>            Path to encrypted salt file
```

**Example:**

```
java -jar Hashing-1.0-jar-with-dependencies.jar --displaySalt
    --privateKey ./keys/private.key --saltFile ./data/project1_stalt_encrypted.txt
```
