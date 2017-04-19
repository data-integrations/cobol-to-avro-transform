# Cobol to Avro Converter

Description
-----------
Cobol to Avro Converter accepts the Cobol Copybook and converts it into the Avro schema. Generated Avro schema
is used to convert the records in the Cobol data file into Apache Avro format.


Use Case
--------

Much of the World’s largest and most critical industries - healthcare, finance, insurance, retail etc. - still generate
huge majority of their data in the mainframe. Storing data in the mainframe involves high cost of maintenance, and need
expertise and special tools to perform data analysis. Offloading the data to Apache Hadoop saves cost, however main
challenge is the lack of connectivity between mainframe and Apache Hadoop. Cobol to Avro Converter provides such connectivity.
It converts mainframe data into Avro format using schema specified by the Copybook.


Properties
----------

**copybook:** The Cobol copybook source code

**codeFormat:** Code format associated with the copybook source code

**charset:** The EBCDIC Charset used to read the data

**rdw:** Specifies whether the Cobol record starts with Record Descriptor Word

**fieldName:** Name of the field containing Cobol records in the form of array of bytes