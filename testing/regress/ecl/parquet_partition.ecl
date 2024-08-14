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

IMPORT Std;
IMPORT Parquet;

// Define record layouts
hiveLayout := RECORD
    INTEGER ID {XPATH('ID')};
    STRING  NAME {XPATH('NAME')};
    INTEGER AGE {XPATH('AGE')};
END;

dirLayout := RECORD
    INTEGER ID {XPATH('ID')};
    STRING  NAME {XPATH('NAME')};
    INTEGER AGE {XPATH('AGE')};
    STRING  COUNTRY {XPATH('COUNTRY')};
END;

// File paths
hiveFilePath1 := '/var/lib/HPCCSystems/mydropzone/hive1.parquet';
dirFilePath1 := '/var/lib/HPCCSystems/mydropzone/directory1.parquet';

// Read data
hiveData1 := ParquetIO.Read(hiveLayout, hiveFilePath1);
dirData1 := ParquetIO.Read(dirLayout, dirFilePath1);

OUTPUT(hiveData1, NAMED('OriginalHiveData'));
OUTPUT(dirData1, NAMED('OriginalDirData'));

// Hive Partitioning
ParquetIO.HivePartition.Write(
    hiveData1,                                    // Data to write
    100000,                                       // Row group size
    '/var/lib/HPCCSystems/mydropzone/hive_partitioned5_new.parquet', // Output path
    TRUE,                                         // Compression
    'ID'                                          // Partition column
);

ReadBackHiveData := ParquetIO.Read(hiveLayout, '/var/lib/HPCCSystems/mydropzone/hive_partitioned5_new.parquet');
HivePartitionResult := IF(SORT(hiveData1, ID) = SORT(ReadBackHiveData, ID),
                          'Pass: Hive Partitioning - Data matches original',
                          'Fail: Hive Partitioning - Data differs from original');
OUTPUT(HivePartitionResult, NAMED('HivePartitioningResult'));

// Directory Partitioning
ParquetIO.DirectoryPartition.Write(
    dirData1,                                    // Data to write
    100000,                                      // Row group size
    '/var/lib/HPCCSystems/mydropzone/dir_partitioned5_new.parquet', // Output path
    TRUE,                                        // Compression
    'ID'                                         // Partition column
);

ReadBackDirData := ParquetIO.Read(dirLayout, '/var/lib/HPCCSystems/mydropzone/dir_partitioned5_new.parquet');
DirectoryPartitionResult := IF(SORT(dirData1, ID) = SORT(ReadBackDirData, ID),
                               'Pass: Directory Partitioning - Data matches original',
                               'Fail: Directory Partitioning - Data differs from original');
OUTPUT(DirectoryPartitionResult, NAMED('DirectoryPartitioningResult'));
