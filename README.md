Cobol to Avro Converter
===================================

![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)
<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=cobol-to-avro-transform"/></a>
[![Build Status](https://travis-ci.org/hydrator/cobol-to-avro-transform.svg?branch=develop)](https://travis-ci.org/hydrator/cobol-to-avro-transform) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) <img src="https://cdap-users.herokuapp.com/assets/cdap-action.svg"/>

Cobol to Avro Converter accepts the Cobol Copybook and converts it into the Avro schema. Generated Avro schema
is used to convert the records in the Cobol data file into Apache Avro format.

Usage Notes
-----------

The plugin accepts a Cobol Copybook as a configuration to generate an Avro schema and use that schema to convert
records into Avro format. Code in the Copybook can be pasted in the "Copybook" property. Avro schema corresponding to
the Copybook can be retrieved using the "Get Schema" button. "Code Format" property is used to specify the code format
in the Copybook, which can either be FIXED_FORMAT or FREE_FORMAT. Charset used to read the data can be specified using
"Charset" property, which defaults to "IBM01140". Cobol data file can contain variable-length logical records, in which case
record consists of a record descriptor word (RDW) followed by the data. Plugin can be configured to use with
variable-length records by setting property "Records start with Record Descriptor Word" to true.

Cobol to Avro Converter is usually used with the WholeFileReader source plugin. WholeFileReader reads the entire data file and pass it
to the converter as an array of bytes. Name of the field containing Cobol records as an array of bytes can be configured.

Plugin Configuration
--------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Copybook**  | **Y** | N/A | Specifies the Cobol Copybook for which Avro schema need to be generated.  |
| **Code Format** | **N** | FIXED_FORMAT | Specifies the format of the Copybook source code.  |
| **Charset** | **N** | IBM01140  | Specifies the EBCDIC charset used to read the data. |
| **Record Descriptor Word** | **N**  | True | Specifies whether the data file contains the variable-length records.  |


Build
-----
To build this plugin:

```
   mvn clean package
```

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/cobol-to-avro-transform-<version>.jar config-file <target/cobol-to-avro-transform-<version>.json>

## Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

## Slack Channel

CDAP Slack Channel: http://cdap-users.herokuapp.com/


## License and Trademarks

Copyright © 2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
