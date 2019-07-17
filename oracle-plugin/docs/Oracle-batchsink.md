# Oracle Batch Sink


Description
-----------
Writes records to an Oracle table. Each record will be written to a row in the table.


Use Case
--------
This sink is used whenever you need to write to an Oracle table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to Oracle table where it can be served to your users.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Driver Name:** Name of the JDBC driver to use.

**Host:** Host that Oracle is running on.

**Port:** Port that Oracle is running on.

**SID/Service Name:** Oracle connection point (Database name or Service name).

**Connection Type** Whether to use an SID or Service Name when connecting to the database.

**Table Name:** Name of the table to export to.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. These arguments
will be passed to the JDBC driver as connection arguments for JDBC drivers that may need additional configurations.

**Default Batch Value:** The default batch value that triggers an execution request.


Data Types Mapping
----------

	| Oracle Data Type               | CDAP Schema Data Type        | Comment                |
	| ------------------------------ | ---------------------------- | ---------------------- |
	| VARCHAR2                       | Type.STRING                  |                        |
	| NVARCHAR2                      | Type.STRING                  |                        |
	| VARCHAR                        | Type.STRING                  |                        |
	| NUMBER                         | LogicalType.DECIMAL          |                        |
	| FLOAT                          | Type.DOUBLE                  |                        |
	| LONG                           | Type.STRING                  |                        |
	| DATE                           | LogicalType.TIMESTAMP_MICROS |                        |
	| BINARY_FLOAT                   | Type.FLOAT                   |                        |
	| BINARY_DOUBLE                  | Type.DOUBLE                  |                        |
	| TIMESTAMP                      | LogicalType.TIMESTAMP_MICROS |                        |
	| TIMESTAMP WITH TIME ZONE       | LogicalType.TIMESTAMP_MICROS | Converted to UTC time  |
	| TIMESTAMP WITH LOCAL TIME ZONE | LogicalType.TIMESTAMP_MICROS |                        |
	| INTERVAL YEAR TO MONTH         | Type.STRING                  |                        |
	| INTERVAL DAY TO SECOND         | Type.STRING                  |                        |
	| RAW                            | Type.BYTES                   |                        |
	| LONG RAW                       | Type.BYTES                   |                        |
	| ROWID                          | Type.STRING                  |                        |
	| UROWID                         | Type.STRING                  |                        |
	| CHAR                           | Type.STRING                  |                        |
	| NCHAR                          | Type.STRING                  |                        |
	| CLOB                           | Type.STRING                  |                        |
	| NCLOB                          | Type.STRING                  |                        |
	| BLOB                           | Type.BYTES                   |                        |
	| BFILE                          | Type.BYTES                   | Java API is deprecated |


Example
-------
Suppose you want to write output records to "users" table of Oracle database named "XE" that is running on "localhost", 
port 1251, as "system" user with "oracle" password (Ensure that the driver for Oracle is installed. You can also provide 
driver name for some specific driver, otherwise "oracle" will be used), then configure the plugin with: 

```
Reference Name: "snk1"
Driver Name: "oracle"
Host: "localhost"
Port: 1251
Database: "XE"
Table Name: "users"
Username: "system"
Password: "oracle"
```
