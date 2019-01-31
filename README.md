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
