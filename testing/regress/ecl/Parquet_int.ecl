//class=ParquetRegression

IMPORT Parquet;

layout := RECORD
    INTEGER1 int1;
    INTEGER2 int2;
    INTEGER4 int4;
    INTEGER8 int8;
    UNSIGNED1 uint1;
    UNSIGNED2 uint2;
    UNSIGNED4 uint4;
    UNSIGNED8 uint8;
END;

signed_ints_test_data := DATASET([
    {-128, -32768, -2147483648, -9223372036854775808, 0, 0, 0, 0},
    {127, 32767, 2147483647, 9223372036854775807, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0, 0}
], layout);

unsigned_ints_test_data := DATASET([
    {0, 0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 255, 65535, 4294967295, 18446744073709551614}
], layout);

random_test_data := DATASET([
    {-42, 16384, -1234567, 123456789, 128, 32000, 2000000000, 8000000000000000000},
    {99, -12345, 987654321, -7890123456, 200, 45000, 3000000000, 10000000000000000000}
], layout);

writeStep :=
    ParquetIO.Write(signed_ints_test_data, '/var/lib/HPCCSystems/mydropzone/signed_ints.parquet', TRUE);
    ParquetIO.Write(unsigned_ints_test_data, '/var/lib/HPCCSystems/mydropzone/unsigned_ints.parquet', TRUE);
    ParquetIO.Write(random_test_data, '/var/lib/HPCCSystems/mydropzone/random.parquet', TRUE);

read_in :=
    ParquetIO.Read(layout, '/var/lib/HPCCSystems/mydropzone/signed_ints.parquet')
    + ParquetIO.Read(layout, '/var/lib/HPCCSystems/mydropzone/unsigned_ints.parquet')
    + ParquetIO.Read(layout, '/var/lib/HPCCSystems/mydropzone/random.parquet');

basic_test_read := OUTPUT(read_in, NAMED('integer_test_output'));

SEQUENTIAL(
    writeStep,
    basic_test_read
);