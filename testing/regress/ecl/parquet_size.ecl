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
    UNSIGNED4 index;
    STRING name;
    STRING director;
END;

// File paths for single datasets
smallFilePath := '/var/lib/HPCCSystems/mydropzone/small_dataset.parquet';
mediumFilePath := '/var/lib/HPCCSystems/mydropzone/medium_dataset.parquet';
largeFilePath := '/var/lib/HPCCSystems/mydropzone/large_dataset.parquet';
largestFilePath := '/var/lib/HPCCSystems/mydropzone/largest_dataset.parquet';

// File paths for multi-part datasets
smallPart1Path := '/var/lib/HPCCSystems/mydropzone/small_dataset_part1.parquet';
smallPart2Path := '/var/lib/HPCCSystems/mydropzone/small_dataset_part2.parquet';

mediumPart1Path := '/var/lib/HPCCSystems/mydropzone/medium_dataset_part1.parquet';
mediumPart2Path := '/var/lib/HPCCSystems/mydropzone/medium_dataset_part2.parquet';

largePart1Path := '/var/lib/HPCCSystems/mydropzone/large_dataset_part1.parquet';
largePart2Path := '/var/lib/HPCCSystems/mydropzone/large_dataset_part2.parquet';

largestPart1Path := '/var/lib/HPCCSystems/mydropzone/largest_dataset_part1.parquet';
largestPart2Path := '/var/lib/HPCCSystems/mydropzone/largest_dataset_part2.parquet';
largestPart3Path := '/var/lib/HPCCSystems/mydropzone/largest_dataset_part3.parquet';

// Read single file datasets
smallDataset := ParquetIO.Read(recordLayout, smallFilePath);
mediumDataset := ParquetIO.Read(recordLayout, mediumFilePath);
largeDataset := ParquetIO.Read(recordLayout, largeFilePath);
largestDataset := ParquetIO.Read(recordLayout, largestFilePath);

// Read multi-part datasets by concatenating the parts
smallPart1 := ParquetIO.Read(recordLayout, smallPart1Path);
smallPart2 := ParquetIO.Read(recordLayout, smallPart2Path);
smallMultiPartDataset := smallPart1 + smallPart2;

mediumPart1 := ParquetIO.Read(recordLayout, mediumPart1Path);
mediumPart2 := ParquetIO.Read(recordLayout, mediumPart2Path);
mediumMultiPartDataset := mediumPart1 + mediumPart2;

largePart1 := ParquetIO.Read(recordLayout, largePart1Path);
largePart2 := ParquetIO.Read(recordLayout, largePart2Path);
largeMultiPartDataset := largePart1 + largePart2;

largestPart1 := ParquetIO.Read(recordLayout, largestPart1Path);
largestPart2 := ParquetIO.Read(recordLayout, largestPart2Path);
largestPart3 := ParquetIO.Read(recordLayout, largestPart3Path);
largestMultiPartDataset := largestPart1 + largestPart2 + largestPart3;

// Compare datasets for equality and return "Pass" or "Fail"
compareSmall := IF(COUNT(smallDataset) = COUNT(smallMultiPartDataset) AND NOT EXISTS(smallDataset - smallMultiPartDataset) AND NOT EXISTS(smallMultiPartDataset - smallDataset), 'Pass', 'Fail');
compareMedium := IF(COUNT(mediumDataset) = COUNT(mediumMultiPartDataset) AND NOT EXISTS(mediumDataset - mediumMultiPartDataset) AND NOT EXISTS(mediumMultiPartDataset - mediumDataset), 'Pass', 'Fail');
compareLarge := IF(COUNT(largeDataset) = COUNT(largeMultiPartDataset) AND NOT EXISTS(largeDataset - largeMultiPartDataset) AND NOT EXISTS(largeMultiPartDataset - largeDataset), 'Pass', 'Fail');
compareLargest := IF(COUNT(largestDataset) = COUNT(largestMultiPartDataset) AND NOT EXISTS(largestDataset - largestMultiPartDataset) AND NOT EXISTS(largestMultiPartDataset - largestDataset), 'Pass', 'Fail');

// Output comparison results
SEQUENTIAL(
    OUTPUT(compareSmall, NAMED('compare_small')),
    OUTPUT(compareMedium, NAMED('compare_medium')),
    OUTPUT(compareLarge, NAMED('compare_large')),
    OUTPUT(compareLargest, NAMED('compare_largest'))
);
