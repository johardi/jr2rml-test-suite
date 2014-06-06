A test suite framework for testing R2RML processors, designed to run all the [W3C R2RML and Direct Mapping Test Cases](http://www.w3.org/TR/rdb2rdf-test-cases/) (only R2RML tests!)

Feature
-------
* Based on JUnit TestSuite style,
* Can be customized on different R2RML processor implementations,
* Produce [EARL reporting output](http://www.w3.org/TR/EARL10-Schema/),
* Open source under GNU GPL v3 license.

Troubleshooting
---------------
TBA

Notes
-----
There are several but minor modifications that I did in the original test suite to allow this tool running the tests in batch.

* Adding `DROP TABLE IF EXISTS` in each *create.sql* file to reset the test data. However, not all DBMS supports this expression and users need to adjust it accordingly.
* Removing double quotes for table or column names in all *r2rml.ttl* test mapping files.

License
-------
This software is licensed under the GPL v3 license, quoted below.
```
JR2RmlTestSuite is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

JR2RmlTestSuite is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.
 
You should have received a copy of the GNU General Public License
along with JR2RmlTestSuite. If not, see http://www.gnu.org/licenses/.
```
