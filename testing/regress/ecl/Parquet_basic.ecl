//class=ParquetRegression

IMPORT Parquet;

layout := RECORD
    INTEGER8 num;
    REAL8 float_val;
    STRING str_val;
    BOOLEAN bool_val;
    STRING date_val;
END;

basic_test_data := DATASET([
    {42, 3.14, 'Hello', TRUE, '2024-05-28'},
    {0, -2.71, 'World', FALSE, '2023-02-28'},
    {-10, 0.0, 'Painting', TRUE, '2022-01-01'}
], layout);

filePath := '/var/lib/HPCCSystems/mydropzone/basic_test.parquet';

writeStep := ParquetIO.Write(basic_test_data, filePath, TRUE);

read_in := ParquetIO.Read(layout, filePath);
basic_test_read := OUTPUT(read_in, NAMED('basic_test_output'));

SEQUENTIAL(
    writeStep,
    basic_test_read
);
