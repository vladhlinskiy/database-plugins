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

    | Oracle Data Type               | CDAP Schema Data Type | Comment                                                |
    | ------------------------------ | --------------------- | ------------------------------------------------------ |
    | VARCHAR2                       | string                |                                                        |
    | NVARCHAR2                      | string                |                                                        |
    | VARCHAR                        | string                |                                                        |
    | NUMBER                         | decimal               |                                                        |
    | FLOAT                          | double                |                                                        |
    | LONG                           | string                |                                                        |
    | DATE                           | timestamp             |                                                        |
    | BINARY_FLOAT                   | float                 |                                                        |
    | BINARY_DOUBLE                  | double                |                                                        |
    | TIMESTAMP                      | timestamp             |                                                        |
    | TIMESTAMP WITH TIME ZONE       | string                | Timestamp string in the following format:              |
    |                                |                       | "2019-07-15 15:57:46.65 GMT"                           |
    | TIMESTAMP WITH LOCAL TIME ZONE | timestamp             |                                                        |
    | INTERVAL YEAR TO MONTH         | string                | Oracle's 'INTERVAL YEAR TO MONTH' literal in the       |
    |                                |                       | standard format: "year[-month]"                        |
    | INTERVAL DAY TO SECOND         | string                | Oracle's 'INTERVAL DAY TO SECOND' literal in the       |
    |                                |                       | standard format:                                       |
    |                                |                       | "[day] [hour][:minutes][:seconds[.milliseconds]"       |
    | RAW                            | bytes                 |                                                        |
    | LONG RAW                       | bytes                 |                                                        |
    | ROWID                          | string                |                                                        |
    | UROWID                         | string                |                                                        |
    | CHAR                           | string                |                                                        |
    | NCHAR                          | string                |                                                        |
    | CLOB                           | string                |                                                        |
    | NCLOB                          | string                |                                                        |
    | BLOB                           | bytes                 |                                                        |
    | BFILE                          | bytes                 | BFILE is a data type used to store a locator (link)    |
    |                                |                       | to an external file, which is stored outside of the    |
    |                                |                       | database. Only the locator will be written to an       |
    |                                |                       | Oracle table and not the content of the external file. |


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
