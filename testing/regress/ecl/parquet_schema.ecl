/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//class=parquet
//nothor,noroxie
//fail
//nokey

IMPORT Std;
IMPORT Parquet;

initialLayout := RECORD
    INTEGER id;
    STRING name;
END;

updatedLayout := RECORD
    INTEGER id;
    STRING name;
    INTEGER age;
END;

initialFilePath := '/var/lib/HPCCSystems/mydropzone/initial.parquet';
updatedFilePath := '/var/lib/HPCCSystems/mydropzone/updated.parquet';

initialData := DATASET([
    { 1, 'Alice' },
    { 2, 'Bob' }
], initialLayout);

ParquetIO.Write(initialData, initialFilePath, TRUE, 'Snappy');

// Write updated dataset to another Parquet file
updatedData := DATASET([
    { 1, 'Alice', 25 },
    { 2, 'Bob', 30 }
], updatedLayout);

ParquetIO.Write(updatedData, updatedFilePath, TRUE, 'Snappy');

initialRead := ParquetIO.Read(initialLayout, initialFilePath);
updatedRead := ParquetIO.Read(updatedLayout, updatedFilePath);

// Compare datasets by joining on the common fields
difference := JOIN(initialRead, updatedRead, LEFT.id = RIGHT.id,
                   TRANSFORM(RECORDOF(updatedRead),
                             SELF.id := LEFT.id,
                             SELF.name := LEFT.name,
                             SELF.age := RIGHT.age),
                   LOOKUP);

// Output the results of the comparison
OUTPUT(difference, NAMED('Differences'));
