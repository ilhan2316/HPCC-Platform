//class=ParquetRegression

IMPORT Parquet;

layout := RECORD
    STRING date_val;
END;

date_test_data := DATASET([
    {'1970-01-01'},
    {'2000-02-29'},
    {'2020-12-31'},
    {'9999-12-31'},
    {'0001-01-01'}
], layout);

filePath := '/var/lib/HPCCSystems/mydropzone/date_test.parquet';
writeStep := ParquetIO.Write(date_test_data, filePath, TRUE);
read_in := ParquetIO.Read(layout, filePath);
date_test_read := OUTPUT(read_in, NAMED('date_test_output'));

SEQUENTIAL(
    writeStep,
    date_test_read
);

