//class=ParquetRegression

IMPORT Parquet, Std.Uni;

RECORDDEF := RECORD
    STRING col1;
END;

filePath1 := '/var/lib/HPCCSystems/mydropzone/empty.parquet';
filePath2 := '/var/lib/HPCCSystems/mydropzone/corrupt.parquet';

EMPTY_PARQUET := ParquetIO.Read(RECORDDEF, filePath1);
CORRUPT_PARQUET := ParquetIO.Read(RECORDDEF, filePath2);

EMPTY_RESULT := IF(COUNT(EMPTY_PARQUET) = 0, DATASET(['Empty Parquet File'], RECORDDEF), EMPTY_PARQUET);
CORRUPT_RESULT := IF(COUNT(CORRUPT_PARQUET) = 0, DATASET(['Empty Parquet File'], RECORDDEF), CORRUPT_PARQUET);

OUTPUT(EMPTY_RESULT);
OUTPUT(CORRUPT_RESULT);









