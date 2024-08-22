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
//Cover's data type's supported by ECL and arrow

IMPORT Std;
IMPORT Parquet;

RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    BOOLEAN value;
END;

booleanDatasetOut := DATASET([
    {000, 'aaa', TRUE},
    {001, 'aab', FALSE}
], RECORDDEF);

ParquetIO.Write(booleanDatasetOut, '/var/lib/HPCCSystems/mydropzone/BooleanTest.parquet', TRUE);

booleanDatasetIn := ParquetIO.Read(RECORDDEF, '/var/lib/HPCCSystems/mydropzone/BooleanTest.parquet');

joinResult := JOIN(booleanDatasetOut, booleanDatasetIn, LEFT.testid = RIGHT.testid AND LEFT.testname = RIGHT.testname AND LEFT.value = RIGHT.value, TRANSFORM(RECORDDEF, SELF := LEFT));

booleanResult := IF(COUNT(booleanDatasetOut) = COUNT(booleanDatasetIn) AND COUNT(joinResult) = COUNT(booleanDatasetOut), 'Pass', 'Fail');

INTEGER_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    INTEGER value;
END;

integerDatasetOut := DATASET([
    {-2147483648, 'min', -2147483648},
    {2147483647, 'max', 2147483647}
], INTEGER_RECORDDEF);

ParquetIO.Write(integerDatasetOut, '/var/lib/HPCCSystems/mydropzone/IntegerTest.parquet', TRUE);

integerDatasetIn := ParquetIO.Read(INTEGER_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/IntegerTest.parquet');

integerResult := IF(
    COUNT(integerDatasetOut) = COUNT(integerDatasetIn) AND
    COUNT(JOIN(integerDatasetOut, integerDatasetIn, LEFT.testid = RIGHT.testid AND LEFT.testname = RIGHT.testname AND LEFT.value = RIGHT.value)) = COUNT(integerDatasetOut),
    'Pass', 'Fail: Integer data mismatch'
);

UNSIGNED_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    UNSIGNED value;
END;

unsignedDatasetOut := DATASET([
    {020, 'aan', 0},
    {021, 'aao', 12345},
    {022, 'aap', 4294967295}
], UNSIGNED_RECORDDEF);

ParquetIO.Write(unsignedDatasetOut, '/var/lib/HPCCSystems/mydropzone/UnsignedTest.parquet', TRUE);

unsignedDatasetIn := ParquetIO.Read(UNSIGNED_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/UnsignedTest.parquet');

unsignedResult := IF(
    COUNT(unsignedDatasetOut) = COUNT(unsignedDatasetIn) AND
    COUNT(JOIN(unsignedDatasetOut, unsignedDatasetIn, LEFT.testid = RIGHT.testid AND LEFT.testname = RIGHT.testname AND LEFT.value = RIGHT.value)) = COUNT(unsignedDatasetOut),
    'Pass', 'Fail: Unsigned data mismatch'
);

// Define schema for REAL type
REAL_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    REAL value;
END;

// Define schema for DECIMAL type
DECIMAL_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    DECIMAL10_2 value;
END;

// Define schema for STRING type
STRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

// REAL type test
realDatasetOut := DATASET([
    {001, 'maxValue', 1.7976931348623157E+308},
    {002, 'minValue', 5.0E-324},
    {003, 'normalValue', -123.456}
], REAL_RECORDDEF);

ParquetIO.Write(realDatasetOut, '/var/lib/HPCCSystems/mydropzone/RealTest.parquet', TRUE);

realDatasetIn := ParquetIO.Read(REAL_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/RealTest.parquet');

realResult := IF(
    COUNT(realDatasetOut) = COUNT(realDatasetIn) AND
    COUNT(JOIN(realDatasetOut, realDatasetIn, LEFT.testid = RIGHT.testid AND LEFT.testname = RIGHT.testname AND LEFT.value = RIGHT.value)) = COUNT(realDatasetOut),
    'Pass', 'Fail: Real data mismatch'
);

// DECIMAL type test
decimalDatasetOut := DATASET([
    {040, 'aax', 12.34D},
    {041, 'aay', -56.78D},
    {044, 'abb', 0.00D}
], DECIMAL_RECORDDEF);

ParquetIO.Write(decimalDatasetOut, '/var/lib/HPCCSystems/mydropzone/DecimalTest.parquet', TRUE);

decimalDatasetIn := ParquetIO.Read(DECIMAL_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/DecimalTest.parquet');

decimalResult := IF(
    COUNT(decimalDatasetOut) = COUNT(decimalDatasetIn) AND
    COUNT(JOIN(decimalDatasetOut, decimalDatasetIn, LEFT.testid = RIGHT.testid AND LEFT.testname = RIGHT.testname AND LEFT.value = RIGHT.value)) = COUNT(decimalDatasetOut),
    'Pass', 'Fail: Decimal data mismatch'
);

// STRING type test
stringDatasetOut := DATASET([
    {050, 'abc', 'Hello'},
    {051, 'abd', 'World'},
    {054, 'abg', 'Types'}
], STRING_RECORDDEF);

ParquetIO.Write(stringDatasetOut, '/var/lib/HPCCSystems/mydropzone/StringTest.parquet', TRUE);

stringDatasetIn := ParquetIO.Read(STRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/StringTest.parquet');

stringResult := IF(
    COUNT(stringDatasetOut) = COUNT(stringDatasetIn) AND
    COUNT(JOIN(stringDatasetOut, stringDatasetIn, LEFT.testid = RIGHT.testid AND LEFT.testname = RIGHT.testname AND LEFT.value = RIGHT.value)) = COUNT(stringDatasetOut),
    'Pass', 'Fail: String data mismatch'
);

// Define record structure for DATA_AS_STRING
DATA_AS_STRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

// Create and write dataset with DATA_AS_STRING values
ParquetIO.Write(DATASET([
    {060, 'abh', (STRING)X'0123456789ABCDEF'},
    {061, 'abi', (STRING)X'FEDCBA9876543210'},
    {062, 'abj', (STRING)X'00FF00FF00FF00FF'}
], DATA_AS_STRING_RECORDDEF), '/var/lib/HPCCSystems/mydropzone/DataTest.parquet', TRUE);

// Read the dataset from the Parquet file
dataAsStringDatasetIn := ParquetIO.Read(DATA_AS_STRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/DataTest.parquet');

// Check result
dataAsStringResult := IF(
    COUNT(dataAsStringDatasetIn) = 5,
    'Pass', 'Fail: Data type data count mismatch'
);

// DATA type test
DATA_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    DATA value;
END;

dataDatasetOut := DATASET([
    {060, 'abh', X'0123456789ABCDEF'},
    {061, 'abi', X'FEDCBA9876543210'},
    {064, 'abl', X'1234567890ABCDEF'}
], DATA_RECORDDEF);

ParquetIO.Write(dataDatasetOut, '/var/lib/HPCCSystems/mydropzone/DataTest.parquet', TRUE);

dataDatasetIn := ParquetIO.Read(DATA_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/DataTest.parquet');

dataResult := IF(
    COUNT(dataDatasetOut) = COUNT(dataDatasetIn) AND
    COUNT(JOIN(dataDatasetOut, dataDatasetIn, LEFT.testid = RIGHT.testid AND LEFT.testname = RIGHT.testname AND LEFT.value = RIGHT.value)) = COUNT(dataDatasetOut),
    'Pass', 'Fail: Data type data mismatch'
);

// Define the record schema for VarString
VARSTRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    VARSTRING value;
END;

varStringDatasetOut := DATASET([
    {070, 'abm', 'VarString1'},
    {071, 'abn', ''},
    {072, 'abo', U'UTF8_测试'}
], VARSTRING_RECORDDEF);

ParquetIO.Write(varStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/VarStringTest.parquet', TRUE);

// Read the dataset from the Parquet file
varStringDatasetIn := ParquetIO.Read(VARSTRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/VarStringTest.parquet');

// Check result
varStringResult := IF(
    COUNT(varStringDatasetOut) = COUNT(varStringDatasetIn) AND
    COUNT(JOIN(varStringDatasetOut, varStringDatasetIn,
               LEFT.testid = RIGHT.testid AND
               LEFT.testname = RIGHT.testname AND
               LEFT.value = RIGHT.value)) = COUNT(varStringDatasetOut),
    'Pass', 'Fail: VarString data mismatch'
);

// Define the record schema for QString
QSTRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    QSTRING value;
END;

qStringDatasetOut := DATASET([
    {080, 'abr', ''},
    {081, 'abs', 'NormalString'},
    {082, 'abt', U'Special_字符'}
], QSTRING_RECORDDEF);

ParquetIO.Write(qStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/QStringTest.parquet', TRUE);

// Read the dataset from the Parquet file
qStringDatasetIn := ParquetIO.Read(QSTRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/QStringTest.parquet');

// Check result
qStringResult := IF(
    COUNT(qStringDatasetOut) = COUNT(qStringDatasetIn) AND
    COUNT(JOIN(qStringDatasetOut, qStringDatasetIn,
               LEFT.testid = RIGHT.testid AND
               LEFT.testname = RIGHT.testname AND
               LEFT.value = RIGHT.value)) = COUNT(qStringDatasetOut),
    'Pass', 'Fail: QString data mismatch'
);

// UTF8 type
ParquetIO.write(DATASET([
    {090, 'abw', U'HelloWorld'},
    {091, 'abx', U'こんにちは'},
    {092, 'aby', U'🚀🌟💬'}
], {UNSIGNED testid, STRING3 testname, UTF8 value}), '/var/lib/HPCCSystems/mydropzone/UTF8Test.parquet', TRUE);

utf8Dataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; UTF8 value}, '/var/lib/HPCCSystems/mydropzone/UTF8Test.parquet');
utf8Result := IF(COUNT(utf8Dataset) = 5, 'Pass', 'Fail: UTF8 data count mismatch');

// UNICODE type
ParquetIO.write(DATASET([
    {100, 'acb', U'Unicode1'},
    {101, 'acc', U'Unicode2'},
    {104, 'acf', U'Unicode5'}
], {UNSIGNED testid, STRING3 testname, UNICODE value}), '/var/lib/HPCCSystems/mydropzone/UnicodeTest.parquet', TRUE);

unicodeDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; UNICODE value}, '/var/lib/HPCCSystems/mydropzone/UnicodeTest.parquet');
unicodeResult := IF(COUNT(unicodeDataset) = 5, 'Pass', 'Fail: Unicode data count mismatch');



SET_OF_INTEGER_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    SET OF INTEGER value;
END;

setOfIntegerDatasetOut := DATASET([
    {110, 'acg', [1,2,3]},
    {113, 'acj', [10,11,12]},
    {114, 'ack', [13,14,15]}
], SET_OF_INTEGER_RECORDDEF);

ParquetIO.Write(setOfIntegerDatasetOut, '/var/lib/HPCCSystems/mydropzone/SetOfIntegerTest.parquet', TRUE);

setOfIntegerDatasetIn := ParquetIO.Read(SET_OF_INTEGER_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/SetOfIntegerTest.parquet');

setOfIntegerResult := IF(
    COUNT(setOfIntegerDatasetOut) = COUNT(setOfIntegerDatasetIn) AND
    COUNT(JOIN(setOfIntegerDatasetOut, setOfIntegerDatasetIn,
               LEFT.testid = RIGHT.testid AND
               LEFT.testname = RIGHT.testname AND
               LEFT.value = RIGHT.value)) = COUNT(setOfIntegerDatasetOut),
    'Pass', 'Fail: Set of Integer data mismatch'
);

REAL8_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    REAL8 value;
END;

real8DatasetOut := DATASET([
    {170, 'adk', 1.23D},
    {171, 'adl', -9.87D},
    {172, 'ado', -1.41421356237309D}
], REAL8_RECORDDEF);

ParquetIO.Write(real8DatasetOut, '/var/lib/HPCCSystems/mydropzone/Real8Test.parquet', TRUE);

real8DatasetIn := ParquetIO.Read(REAL8_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/Real8Test.parquet');

real8Result := IF(
    COUNT(real8DatasetOut) = COUNT(real8DatasetIn) AND
    COUNT(JOIN(real8DatasetOut, real8DatasetIn,
               LEFT.testid = RIGHT.testid AND
               LEFT.testname = RIGHT.testname AND
               LEFT.value = RIGHT.value)) = COUNT(real8DatasetOut),
    'Pass', 'Fail: Real8 data mismatch'
);

SET_OF_STRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    SET OF STRING value;
END;

setOfStringDatasetOut := DATASET([
    {180, 'adp', ['Set', 'Of', 'String', 'Test']},
    {181, 'adq', ['ECL', 'Data', 'Types']},
    {184, 'adt', ['A', 'B', 'C', 'D', 'E']}
], SET_OF_STRING_RECORDDEF);

ParquetIO.Write(setOfStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/SetOfStringTest.parquet', TRUE);

setOfStringDatasetIn := ParquetIO.Read(SET_OF_STRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/SetOfStringTest.parquet');

setOfStringResult := IF(
    COUNT(setOfStringDatasetOut) = COUNT(setOfStringDatasetIn) AND
    COUNT(JOIN(setOfStringDatasetOut, setOfStringDatasetIn,
               LEFT.testid = RIGHT.testid AND
               LEFT.testname = RIGHT.testname AND
               LEFT.value = RIGHT.value)) = COUNT(setOfStringDatasetOut),
    'Pass', 'Fail: Set of String data mismatch'
);

SET_OF_UNICODE_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

setOfUnicodeDatasetOut := DATASET([
    {192, 'adw', U'Á,É,Í,Ó,Ú'},
    {193, 'adx', U'α,β,γ,δ,ε'},
    {194, 'ady', U'☀,☁,☂,☃,☄'}
], SET_OF_UNICODE_RECORDDEF);

ParquetIO.Write(setOfUnicodeDatasetOut, '/var/lib/HPCCSystems/mydropzone/SetOfUnicodeTest.parquet', TRUE);

setOfUnicodeDatasetIn := ParquetIO.Read(SET_OF_UNICODE_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/SetOfUnicodeTest.parquet');

setOfUnicodeResult := IF(
    EXISTS(setOfUnicodeDatasetIn) AND
    COUNT(setOfUnicodeDatasetOut) = COUNT(setOfUnicodeDatasetIn) AND
    COUNT(JOIN(setOfUnicodeDatasetOut, setOfUnicodeDatasetIn,
               LEFT.testid = RIGHT.testid AND
               LEFT.testname = RIGHT.testname AND
               LEFT.value = RIGHT.value)) = COUNT(setOfUnicodeDatasetOut),
    'Pass','Fail: Set of Unicode data mismatch'
);

INTEGER8_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    INTEGER8 value;
END;

integer8DatasetOut := DATASET([
    {300, 'afa', (INTEGER8)32767},
    {301, 'afb', (INTEGER8)2147483647},
    {302, 'afc', (INTEGER8)9223372036854775807}
], INTEGER8_RECORDDEF);

ParquetIO.Write(integer8DatasetOut, '/var/lib/HPCCSystems/mydropzone/IntegerSizesTest.parquet', TRUE);

integer8DatasetIn := ParquetIO.Read(INTEGER8_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/IntegerSizesTest.parquet');

integer8Result := IF(
    EXISTS(integer8DatasetIn) AND
    COUNT(integer8DatasetOut) = COUNT(integer8DatasetIn) AND
    COUNT(JOIN(integer8DatasetOut, integer8DatasetIn,
               LEFT.testid = RIGHT.testid AND
               LEFT.testname = RIGHT.testname AND
               LEFT.value = RIGHT.value)) = COUNT(integer8DatasetOut),
    'Pass','Fail: Integer8 data mismatch'
);

UNSIGNED8_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

unsigned8DatasetOut := DATASET([
    {310, 'afd', (STRING)(UNSIGNED8)65535},
    {311, 'afe', (STRING)(UNSIGNED8)4294967295},
    {312, 'aff', (STRING)(UNSIGNED8)18446744073709551615}
], UNSIGNED8_RECORDDEF);

ParquetIO.Write(unsigned8DatasetOut, '/var/lib/HPCCSystems/mydropzone/UnsignedSizesTest.parquet', TRUE);

unsigned8DatasetIn := ParquetIO.Read(UNSIGNED8_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/UnsignedSizesTest.parquet');

unsigned8Result := IF(
    EXISTS(unsigned8DatasetIn) AND
    COUNT(unsigned8DatasetOut) = COUNT(unsigned8DatasetIn) AND
    COUNT(JOIN(unsigned8DatasetOut, unsigned8DatasetIn,
               LEFT.testid = RIGHT.testid AND
               LEFT.testname = RIGHT.testname AND
               LEFT.value = RIGHT.value)) = COUNT(unsigned8DatasetOut),
    'Pass','Fail: Unsigned8 data mismatch'
);

REAL4_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    REAL4 value;
END;

real4DatasetOut := DATASET([
    {320, 'afg', (REAL4)1.23},
    {321, 'afh', (REAL4)-9.87},
    {322, 'afi', (REAL4)3.14159}
], REAL4_RECORDDEF);

ParquetIO.Write(real4DatasetOut, '/var/lib/HPCCSystems/mydropzone/Real4Test.parquet', TRUE);

real4DatasetIn := ParquetIO.Read(REAL4_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/Real4Test.parquet');

real4Result := IF(
    EXISTS(real4DatasetIn) AND
    COUNT(real4DatasetOut) = COUNT(real4DatasetIn) AND
    COUNT(JOIN(real4DatasetOut, real4DatasetIn,
               LEFT.testid = RIGHT.testid AND
               LEFT.testname = RIGHT.testname AND
               LEFT.value = RIGHT.value)) = COUNT(real4DatasetOut),
    'Pass','Fail: Real4 data mismatch'
);

INTEGER1_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    INTEGER1 value;
END;

integer1DatasetOut := DATASET([
    {340, 'afp', 127},
    {341, 'afq', -128},
    {342, 'afr', 0}
], INTEGER1_RECORDDEF);

ParquetIO.Write(integer1DatasetOut, '/var/lib/HPCCSystems/mydropzone/Integer1Test.parquet', TRUE);

integer1DatasetIn := ParquetIO.Read(INTEGER1_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/Integer1Test.parquet');

integer1Result := IF(
    EXISTS(integer1DatasetIn) AND
    COUNT(integer1DatasetOut) = COUNT(integer1DatasetIn) AND
    COUNT(JOIN(integer1DatasetOut, integer1DatasetIn,
               LEFT.testid = RIGHT.testid AND
               LEFT.testname = RIGHT.testname AND
               LEFT.value = RIGHT.value)) = COUNT(integer1DatasetOut),
    'Pass', 'Fail: Integer1 data mismatch'
);

DATA10_RECORDDEF := RECORD
    UNSIGNED1 id;
    STRING3 name;
    DATA10 value;
END;

DATA10 REALToBinary(REAL val) := (DATA10)val;

dataset_fixed_size_binaryOut := DATASET([
    {1, 'pos', REALToBinary(3.14159)},
    {2, 'neg', REALToBinary(-2.71828)}
], DATA10_RECORDDEF);

ParquetIO.Write(dataset_fixed_size_binaryOut, '/var/lib/HPCCSystems/mydropzone/FixedSizeBinaryTest.parquet', TRUE);

fixedSizeBinaryDatasetIn := ParquetIO.Read(DATA10_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/FixedSizeBinaryTest.parquet');

fixedSizeBinaryResult := IF(
    EXISTS(fixedSizeBinaryDatasetIn) AND
    COUNT(dataset_fixed_size_binaryOut) = COUNT(fixedSizeBinaryDatasetIn) AND
    COUNT(JOIN(dataset_fixed_size_binaryOut, fixedSizeBinaryDatasetIn,
               LEFT.id = RIGHT.id AND
               LEFT.name = RIGHT.name AND
               LEFT.value = RIGHT.value)) = COUNT(dataset_fixed_size_binaryOut),
    'Pass', 'Fail: Fixed Size Binary data mismatch'
);

// Large Binary
LARGE_BINARY_RECORDDEF := RECORD
    UNSIGNED1 id;
    STRING3 name;
    DATA value;
END;

DATA REALToLargeBinary(REAL val) := (DATA)val;

dataset_large_binaryOut := DATASET([
    {1, 'pos', REALToLargeBinary(3.14159)},
    {2, 'neg', REALToLargeBinary(-2.71828)}
], LARGE_BINARY_RECORDDEF);

ParquetIO.Write(dataset_large_binaryOut, '/var/lib/HPCCSystems/mydropzone/LargeBinaryTest.parquet', TRUE);

largeBinaryDatasetIn := ParquetIO.Read(LARGE_BINARY_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/LargeBinaryTest.parquet');

largeBinaryResult := IF(
    EXISTS(largeBinaryDatasetIn) AND
    COUNT(dataset_large_binaryOut) = COUNT(largeBinaryDatasetIn) AND
    COUNT(JOIN(dataset_large_binaryOut, largeBinaryDatasetIn,
               LEFT.id = RIGHT.id AND
               LEFT.name = RIGHT.name AND
               LEFT.value = RIGHT.value)) = COUNT(dataset_large_binaryOut),
    'Pass','Fail: Large Binary data mismatch'
);

// Large List
LIST_RECORDDEF := RECORD
    UNSIGNED1 id;
    STRING4 name;
    STRING value;
END;

dataset_large_listOut := DATASET([
    {1, 'lst1', 'apple,banana,cherry'},
    {2, 'lst2', 'dog,cat,bird,fish'},
    {3, 'lst3', 'red,green,blue,yellow,purple'}
], LIST_RECORDDEF);

ParquetIO.Write(dataset_large_listOut, '/var/lib/HPCCSystems/mydropzone/LargeListTest.parquet', TRUE);

largeListDatasetIn := ParquetIO.Read(LIST_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/LargeListTest.parquet');

largeListResult := IF(
    EXISTS(largeListDatasetIn) AND
    COUNT(dataset_large_listOut) = COUNT(largeListDatasetIn) AND
    COUNT(JOIN(dataset_large_listOut, largeListDatasetIn,
               LEFT.id = RIGHT.id AND
               LEFT.name = RIGHT.name AND
               LEFT.value = RIGHT.value)) = COUNT(dataset_large_listOut),
    'Pass','Fail: Large List data mismatch'
);


PARALLEL(
    OUTPUT(booleanResult, NAMED('BooleanTest'), OVERWRITE),
    OUTPUT(integerResult, NAMED('IntegerTest'), OVERWRITE),
    OUTPUT(unsignedResult, NAMED('UnsignedTest'), OVERWRITE),
    OUTPUT(realResult, NAMED('RealTest'), OVERWRITE),
    OUTPUT(decimalResult, NAMED('DecimalTest'), OVERWRITE),
    OUTPUT(stringResult, NAMED('StringTest'), OVERWRITE),
    OUTPUT(dataAsStringResult, NAMED('DataAsStringTest'), OVERWRITE),
    OUTPUT(varStringResult, NAMED('VarStringTest'), OVERWRITE),
    OUTPUT(qStringResult, NAMED('QStringTest'), OVERWRITE),
    OUTPUT(utf8Result, NAMED('UTF8Test'), OVERWRITE),
    OUTPUT(unicodeResult, NAMED('UnicodeTest'), OVERWRITE),
    OUTPUT(setOfIntegerResult, NAMED('SetOfIntegerTest'), OVERWRITE),
    OUTPUT(real8Result, NAMED('Real8Test'), OVERWRITE),
    OUTPUT(setOfStringResult, NAMED('SetOfStringTest'), OVERWRITE),
    OUTPUT(setOfUnicodeResult, NAMED('SetOfUnicodeTest'), OVERWRITE),
    OUTPUT(integer8Result, NAMED('IntegerSizesTest'), OVERWRITE),
    OUTPUT(unsigned8Result, NAMED('UnsignedSizesTest'), OVERWRITE),
    OUTPUT(real4Result, NAMED('Real4Test'), OVERWRITE),
    OUTPUT(integer1Result, NAMED('Integer1Test'), OVERWRITE),
    OUTPUT(fixedSizeBinaryResult, NAMED('FixedSizeBinaryTest'), OVERWRITE),
    OUTPUT(largeBinaryResult, NAMED('LargeBinaryTest'), OVERWRITE),
    OUTPUT(largeListResult, NAMED('LargeListTest'), OVERWRITE)
);
