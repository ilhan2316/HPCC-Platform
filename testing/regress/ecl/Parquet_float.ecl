//class=ParquetRegression

IMPORT Parquet;

recordLayout := RECORD
    REAL4 sepal_length;
    REAL4 sepal_width;
    REAL4 petal_length;
    REAL4 petal_width;
    STRING species;
END;

filePath := '/var/lib/HPCCSystems/mydropzone/float_dataset.parquet';

importedDataset := ParquetIO.Read(recordLayout, filePath);

writeStep := ParquetIO.Write(importedDataset, filePath, TRUE);

outputDataset := ParquetIO.Read(recordLayout, filePath);

SEQUENTIAL(
    writeStep,
    OUTPUT(outputDataset, NAMED('output_dataset'))
);




