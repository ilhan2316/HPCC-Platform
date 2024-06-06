//class=ParquetRegression

IMPORT Parquet;

boolLayout := RECORD
    BOOLEAN boolVal;
END;

inputData := DATASET([
    {true},
    {false}
], boolLayout);

filePath := '/var/lib/HPCCSystems/mydropzone/boolean_test.parquet';

writeStep := ParquetIO.Write(inputData, filePath, TRUE);

dropzoneBoolData := ParquetIO.Read(boolLayout, filePath);

SEQUENTIAL(
    writeStep,
    OUTPUT(inputData, NAMED('input_data')),
    OUTPUT(dropzoneBoolData, NAMED('parquet_data'))
);




