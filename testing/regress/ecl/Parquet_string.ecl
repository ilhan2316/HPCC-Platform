//class=ParquetRegression

IMPORT Parquet;

recordLayout := RECORD
    UNSIGNED4 index;
    STRING name;
    STRING director;
END;

filePath := '/var/lib/HPCCSystems/mydropzone/string_dataset.parquet';

importedDataset := ParquetIO.Read(recordLayout, filePath);

writeStep := ParquetIO.Write(importedDataset, filePath, TRUE);

outputDataset := ParquetIO.Read(recordLayout, filePath);

SEQUENTIAL(
    writeStep,
    OUTPUT(outputDataset, NAMED('output_dataset'))
);


