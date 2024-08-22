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

IMPORT Parquet;

recordLayout := RECORD
    UNSIGNED4 id;
    STRING name;
    REAL8 price;
    STRING isactive;
END;

smallFilePath := '/var/lib/HPCCSystems/mydropzone/small1.parquet';
mediumFilePath := '/var/lib/HPCCSystems/mydropzone/medium1.parquet';

smallDataset := ParquetIO.Read(recordLayout, smallFilePath);
largeDataset := ParquetIO.Read(recordLayout, mediumFilePath);

largeDatasetPart1 := largeDataset[1..33];
largeDatasetPart2 := largeDataset[34..66];
largeDatasetPart3 := largeDataset[67..100];

combinedLargeDataset := largeDatasetPart1 + largeDatasetPart2 + largeDatasetPart3;

SEQUENTIAL(
    OUTPUT(smallDataset, NAMED('small_dataset')),
    OUTPUT(combinedLargeDataset, NAMED('large_dataset'))
);
