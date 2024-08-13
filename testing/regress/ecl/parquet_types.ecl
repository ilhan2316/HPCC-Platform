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
//nothor,noroxie
//Cover's data type's supported by ECL and arrow

IMPORT Parquet;

// Define schema
RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    BOOLEAN value;
END;

// Create and write dataset
booleanDatasetOut := DATASET([
    {000, 'aaa', TRUE},
    {001, 'aab', FALSE},
    {002, 'aac', TRUE},
    {003, 'aad', FALSE},
    {004, 'aae', TRUE}
], RECORDDEF);

ParquetIO.Write(booleanDatasetOut, '/var/lib/HPCCSystems/mydropzone/BooleanTest.parquet', TRUE);

// Read dataset from Parquet file
booleanDatasetIn := ParquetIO.Read(RECORDDEF, '/var/lib/HPCCSystems/mydropzone/BooleanTest.parquet');

// Compare datasets
booleanDatasetOutSorted := SORT(booleanDatasetOut, testid);
booleanDatasetInSorted := SORT(booleanDatasetIn, testid);

booleanResult := IF(
    EXISTS(booleanDatasetIn) AND
    COUNT(booleanDatasetOutSorted) = COUNT(booleanDatasetInSorted) AND
    booleanDatasetOutSorted = booleanDatasetInSorted,
    'Pass',
    'Fail'
);

INTEGER_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    INTEGER value;
END;

// Create and write dataset
integerDatasetOut := DATASET([
    {010, 'aai', 123},
    {011, 'aaj', -987},
    {012, 'aak', 456},
    {013, 'aal', 789},
    {014, 'aam', -321}
], INTEGER_RECORDDEF);

ParquetIO.Write(integerDatasetOut, '/var/lib/HPCCSystems/mydropzone/IntegerTest.parquet', TRUE);

integerDatasetIn := ParquetIO.Read(INTEGER_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/IntegerTest.parquet');

integerDatasetOutSorted := SORT(integerDatasetOut, testid);
integerDatasetInSorted := SORT(integerDatasetIn, testid);

integerResult := IF(
    EXISTS(integerDatasetIn) AND
    COUNT(integerDatasetOutSorted) = COUNT(integerDatasetInSorted) AND
    integerDatasetOutSorted = integerDatasetInSorted,
    'Pass',
    'Fail: Integer data mismatch'
);

UNSIGNED_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    UNSIGNED value;
END;

// Create and write dataset
unsignedDatasetOut := DATASET([
    {020, 'aan', 12345},
    {021, 'aao', 67890},
    {022, 'aap', 1234},
    {023, 'aaq', 5678},
    {024, 'aar', 91011}
], UNSIGNED_RECORDDEF);

ParquetIO.Write(unsignedDatasetOut, '/var/lib/HPCCSystems/mydropzone/UnsignedTest.parquet', TRUE);

unsignedDatasetIn := ParquetIO.Read(UNSIGNED_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/UnsignedTest.parquet');

unsignedDatasetOutSorted := SORT(unsignedDatasetOut, testid);
unsignedDatasetInSorted := SORT(unsignedDatasetIn, testid);

unsignedResult := IF(
    EXISTS(unsignedDatasetIn) AND
    COUNT(unsignedDatasetOutSorted) = COUNT(unsignedDatasetInSorted) AND
    unsignedDatasetOutSorted = unsignedDatasetInSorted,
    'Pass',
    'Fail: Unsigned data mismatch'
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
    {030, 'aas', 1.23},
    {031, 'aat', -9.87},
    {032, 'aau', 45.67},
    {033, 'aav', 78.90},
    {034, 'aaw', -32.1}
], REAL_RECORDDEF);

ParquetIO.Write(realDatasetOut, '/var/lib/HPCCSystems/mydropzone/RealTest.parquet', TRUE);

realDatasetIn := ParquetIO.Read(REAL_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/RealTest.parquet');

realDatasetOutSorted := SORT(realDatasetOut, testid);
realDatasetInSorted := SORT(realDatasetIn, testid);

realResult := IF(
    EXISTS(realDatasetIn) AND
    COUNT(realDatasetOutSorted) = COUNT(realDatasetInSorted) AND
    realDatasetOutSorted = realDatasetInSorted,
    'Pass',
    'Fail: Real data mismatch'
);

// DECIMAL type test
decimalDatasetOut := DATASET([
    {040, 'aax', 12.34D},
    {041, 'aay', -56.78D},
    {042, 'aaz', 90.12D},
    {043, 'aba', 34.56D},
    {044, 'abb', -78.90D}
], DECIMAL_RECORDDEF);

ParquetIO.Write(decimalDatasetOut, '/var/lib/HPCCSystems/mydropzone/DecimalTest.parquet', TRUE);

decimalDatasetIn := ParquetIO.Read(DECIMAL_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/DecimalTest.parquet');

decimalDatasetOutSorted := SORT(decimalDatasetOut, testid);
decimalDatasetInSorted := SORT(decimalDatasetIn, testid);

decimalResult := IF(
    EXISTS(decimalDatasetIn) AND
    COUNT(decimalDatasetOutSorted) = COUNT(decimalDatasetInSorted) AND
    decimalDatasetOutSorted = decimalDatasetInSorted,
    'Pass',
    'Fail: Decimal data mismatch'
);

// STRING type test
stringDatasetOut := DATASET([
    {050, 'abc', 'Hello'},
    {051, 'abd', 'World'},
    {052, 'abe', 'Test'},
    {053, 'abf', 'String'},
    {054, 'abg', 'Types'}
], STRING_RECORDDEF);

ParquetIO.Write(stringDatasetOut, '/var/lib/HPCCSystems/mydropzone/StringTest.parquet', TRUE);

stringDatasetIn := ParquetIO.Read(STRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/StringTest.parquet');

stringDatasetOutSorted := SORT(stringDatasetOut, testid);
stringDatasetInSorted := SORT(stringDatasetIn, testid);

stringResult := IF(
    EXISTS(stringDatasetIn) AND
    COUNT(stringDatasetOutSorted) = COUNT(stringDatasetInSorted) AND
    stringDatasetOutSorted = stringDatasetInSorted,
    'Pass',
    'Fail: String data mismatch'
);

// DATA type test
DATA_AS_STRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

dataAsStringDatasetOut := DATASET([
    {060, 'abh', (STRING)X'0123456789ABCDEF'},
    {061, 'abi', (STRING)X'FEDCBA9876543210'},
    {062, 'abj', (STRING)X'00FF00FF00FF00FF'},
    {063, 'abk', (STRING)X'FF00FF00FF00FF00'},
    {064, 'abl', (STRING)X'1234567890ABCDEF'}
], DATA_AS_STRING_RECORDDEF);

ParquetIO.Write(dataAsStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/DataTest.parquet', TRUE);

dataAsStringDatasetIn := ParquetIO.Read(DATA_AS_STRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/DataTest.parquet');

dataAsStringDatasetOutSorted := SORT(dataAsStringDatasetOut, testid);
dataAsStringDatasetInSorted := SORT(dataAsStringDatasetIn, testid);

dataAsStringResult := IF(
    EXISTS(dataAsStringDatasetIn) AND
    COUNT(dataAsStringDatasetOutSorted) = COUNT(dataAsStringDatasetInSorted) AND
    dataAsStringDatasetOutSorted = dataAsStringDatasetInSorted,
    'Pass',
    'Fail: Data type data mismatch'
);

// Define the record schema for VarString
VARSTRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    VARSTRING value;
END;

// Create and write dataset with VARSTRING values
varStringDatasetOut := DATASET([
    {070, 'abm', 'VarString1'},
    {071, 'abn', 'VarString2'},
    {072, 'abo', 'VarString3'},
    {073, 'abp', 'VarString4'},
    {074, 'abq', 'VarString5'}
], VARSTRING_RECORDDEF);

ParquetIO.Write(varStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/VarStringTest.parquet', TRUE);

// Read the dataset from the Parquet file
varStringDatasetIn := ParquetIO.Read(VARSTRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/VarStringTest.parquet');

// Sort and compare datasets
varStringDatasetOutSorted := SORT(varStringDatasetOut, testid);
varStringDatasetInSorted := SORT(varStringDatasetIn, testid);

varStringResult := IF(
    EXISTS(varStringDatasetIn) AND
    COUNT(varStringDatasetOutSorted) = COUNT(varStringDatasetInSorted) AND
    varStringDatasetOutSorted = varStringDatasetInSorted,
    'Pass',
    'Fail: VarString data mismatch'
);

// Define the record schema for QString
QSTRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    QSTRING value;
END;

// Create and write dataset with QString values
qStringDatasetOut := DATASET([
    {080, 'abr', 'QStr1'},
    {081, 'abs', 'QStr2'},
    {082, 'abt', 'QStr3'},
    {083, 'abu', 'QStr4'},
    {084, 'abv', 'QStr5'}
], QSTRING_RECORDDEF);

ParquetIO.Write(qStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/QStringTest.parquet', TRUE);

// Read the dataset from the Parquet file
qStringDatasetIn := ParquetIO.Read(QSTRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/QStringTest.parquet');

// Sort and compare datasets
qStringDatasetOutSorted := SORT(qStringDatasetOut, testid);
qStringDatasetInSorted := SORT(qStringDatasetIn, testid);

qStringResult := IF(
    EXISTS(qStringDatasetIn) AND
    COUNT(qStringDatasetOutSorted) = COUNT(qStringDatasetInSorted) AND
    qStringDatasetOutSorted = qStringDatasetInSorted,
    'Pass',
    'Fail: QString data mismatch'
);

// UTF8 type
ParquetIO.write(DATASET([
    {090, 'abw', U'UTF8_1'},
    {091, 'abx', U'UTF8_2'},
    {092, 'aby', U'UTF8_3'},
    {093, 'abz', U'UTF8_4'},
    {094, 'aca', U'UTF8_5'}
], {UNSIGNED testid, STRING3 testname, UTF8 value}), '/var/lib/HPCCSystems/mydropzone/UTF8Test.parquet', TRUE);

utf8Dataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; UTF8 value}, '/var/lib/HPCCSystems/mydropzone/UTF8Test.parquet');
utf8Result := IF(COUNT(utf8Dataset) = 5, 'Pass', 'Fail: UTF8 data count mismatch');

// UNICODE type
ParquetIO.write(DATASET([
    {100, 'acb', U'Unicode1'},
    {101, 'acc', U'Unicode2'},
    {102, 'acd', U'Unicode3'},
    {103, 'ace', U'Unicode4'},
    {104, 'acf', U'Unicode5'}
], {UNSIGNED testid, STRING3 testname, UNICODE value}), '/var/lib/HPCCSystems/mydropzone/UnicodeTest.parquet', TRUE);

unicodeDataset := ParquetIO.Read({UNSIGNED testid; STRING3 testname; UNICODE value}, '/var/lib/HPCCSystems/mydropzone/UnicodeTest.parquet');
unicodeResult := IF(COUNT(unicodeDataset) = 5, 'Pass', 'Fail: Unicode data count mismatch');

// Define the record schema for SET OF INTEGER
SET_OF_INTEGER_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    SET OF INTEGER value;
END;

// Create and write dataset with SET OF INTEGER values
setOfIntegerDatasetOut := DATASET([
    {110, 'acg', [1,2,3]},
    {111, 'ach', [4,5,6]},
    {112, 'aci', [7,8,9]},
    {113, 'acj', [10,11,12]},
    {114, 'ack', [13,14,15]}
], SET_OF_INTEGER_RECORDDEF);

ParquetIO.Write(setOfIntegerDatasetOut, '/var/lib/HPCCSystems/mydropzone/SetOfIntegerTest.parquet', TRUE);

setOfIntegerDatasetIn := ParquetIO.Read(SET_OF_INTEGER_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/SetOfIntegerTest.parquet');

setOfIntegerDatasetOutSorted := SORT(setOfIntegerDatasetOut, testid);
setOfIntegerDatasetInSorted := SORT(setOfIntegerDatasetIn, testid);

setOfIntegerResult := IF(
    EXISTS(setOfIntegerDatasetIn) AND
    COUNT(setOfIntegerDatasetOutSorted) = COUNT(setOfIntegerDatasetInSorted) AND
    setOfIntegerDatasetOutSorted = setOfIntegerDatasetInSorted,
    'Pass',
    'Fail: Set of Integer data mismatch'
);

// REAL8 (FLOAT8) type test
REAL8_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

real8DatasetOut := DATASET([
    {170, 'adk', (STRING)1.23D},
    {171, 'adl', (STRING)-9.87D},
    {172, 'adm', (STRING)3.14159265358979D},
    {173, 'adn', (STRING)2.71828182845904D},
    {174, 'ado', (STRING)-1.41421356237309D}
], REAL8_RECORDDEF);

ParquetIO.Write(real8DatasetOut, '/var/lib/HPCCSystems/mydropzone/Real8Test.parquet', TRUE);

real8DatasetIn := ParquetIO.Read(REAL8_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/Real8Test.parquet');

real8DatasetOutSorted := SORT(real8DatasetOut, testid);
real8DatasetInSorted := SORT(real8DatasetIn, testid);

real8Result := IF(
    EXISTS(real8DatasetIn) AND
    COUNT(real8DatasetOutSorted) = COUNT(real8DatasetInSorted) AND
    real8DatasetOutSorted = real8DatasetInSorted,
    'Pass',
    'Fail: Real8 data mismatch'
);

// SET OF STRING
SET_OF_STRING_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    SET OF STRING value;
END;

// SET OF STRING values
setOfStringDatasetOut := DATASET([
    {180, 'adp', ['Set', 'Of', 'String', 'Test']},
    {181, 'adq', ['ECL', 'Data', 'Types']},
    {182, 'adr', ['Hello', 'World']},
    {183, 'ads', ['One', 'Two', 'Three', 'Four', 'Five']},
    {184, 'adt', ['A', 'B', 'C', 'D', 'E']}
], SET_OF_STRING_RECORDDEF);

ParquetIO.Write(setOfStringDatasetOut, '/var/lib/HPCCSystems/mydropzone/SetOfStringTest.parquet', TRUE);

setOfStringDatasetIn := ParquetIO.Read(SET_OF_STRING_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/SetOfStringTest.parquet');

setOfStringDatasetOutSorted := SORT(setOfStringDatasetOut, testid);
setOfStringDatasetInSorted := SORT(setOfStringDatasetIn, testid);

setOfStringResult := IF(
    EXISTS(setOfStringDatasetIn) AND
    COUNT(setOfStringDatasetOutSorted) = COUNT(setOfStringDatasetInSorted) AND
    setOfStringDatasetOutSorted = setOfStringDatasetInSorted,
    'Pass',
    'Fail: Set of String data mismatch'
);

// Define the record schema for the dataset
SET_OF_UNICODE_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

// Create and write the dataset with Unicode values concatenated into a single STRING
setOfUnicodeDatasetOut := DATASET([
    {190, 'adu', 'Unicode,Set,Test'},
    {192, 'adw', U'Á,É,Í,Ó,Ú'},
    {193, 'adx', U'α,β,γ,δ,ε'},
    {194, 'ady', U'☀,☁,☂,☃,☄'}
], SET_OF_UNICODE_RECORDDEF);

ParquetIO.Write(setOfUnicodeDatasetOut, '/var/lib/HPCCSystems/mydropzone/SetOfUnicodeTest.parquet', TRUE);

setOfUnicodeDatasetIn := ParquetIO.Read(SET_OF_UNICODE_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/SetOfUnicodeTest.parquet');

setOfUnicodeDatasetOutSorted := SORT(setOfUnicodeDatasetOut, testid);
setOfUnicodeDatasetInSorted := SORT(setOfUnicodeDatasetIn, testid);

setOfUnicodeResult := IF(
    EXISTS(setOfUnicodeDatasetIn) AND
    COUNT(setOfUnicodeDatasetOutSorted) = COUNT(setOfUnicodeDatasetInSorted) AND
    setOfUnicodeDatasetOutSorted = setOfUnicodeDatasetInSorted,
    'Pass',
    'Fail: Set of Unicode data mismatch'
);

// INTEGER8
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

integer8DatasetOutSorted := SORT(integer8DatasetOut, testid);
integer8DatasetInSorted := SORT(integer8DatasetIn, testid);

integer8Result := IF(
    EXISTS(integer8DatasetIn) AND
    COUNT(integer8DatasetOutSorted) = COUNT(integer8DatasetInSorted) AND
    integer8DatasetOutSorted = integer8DatasetInSorted,
    'Pass',
    'Fail: Integer8 data mismatch'
);


UNSIGNED8_RECORDDEF := RECORD
    UNSIGNED testid;
    STRING3 testname;
    STRING value;
END;

// Create and write dataset
unsigned8DatasetOut := DATASET([
    {310, 'afd', (STRING)(UNSIGNED8)65535},
    {311, 'afe', (STRING)(UNSIGNED8)4294967295},
    {312, 'aff', (STRING)(UNSIGNED8)18446744073709551615}
], UNSIGNED8_RECORDDEF);

ParquetIO.Write(unsigned8DatasetOut, '/var/lib/HPCCSystems/mydropzone/UnsignedSizesTest.parquet', TRUE);

unsigned8DatasetIn := ParquetIO.Read(UNSIGNED8_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/UnsignedSizesTest.parquet');

unsigned8DatasetOutSorted := SORT(unsigned8DatasetOut, testid);
unsigned8DatasetInSorted := SORT(unsigned8DatasetIn, testid);

unsigned8Result := IF(
    EXISTS(unsigned8DatasetIn) AND
    COUNT(unsigned8DatasetOutSorted) = COUNT(unsigned8DatasetInSorted) AND
    unsigned8DatasetOutSorted = unsigned8DatasetInSorted,
    'Pass',
    'Fail: Unsigned8 data mismatch'
);

// REAL4
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

real4DatasetOutSorted := SORT(real4DatasetOut, testid);
real4DatasetInSorted := SORT(real4DatasetIn, testid);

real4Result := IF(
    EXISTS(real4DatasetIn) AND
    COUNT(real4DatasetOutSorted) = COUNT(real4DatasetInSorted) AND
    real4DatasetOutSorted = real4DatasetInSorted,
    'Pass',
    'Fail: Real4 data mismatch'
);


// INTEGER1 (BYTE) type
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
    COUNT(integer1DatasetIn) = 3,
    'Pass',
    'Fail: Integer1 data count mismatch'
);

DATA10_RECORDDEF := RECORD
    UNSIGNED1 id;
    STRING3 name;
    DATA10 value;
END;

DATA10 REALToBinary(REAL val) := (DATA10)val;

dataset_fixed_size_binaryOut := DATASET([
    {1, 'pos', REALToBinary(3.14159)},
    {2, 'neg', REALToBinary(-2.71828)},
    {3, 'zer', REALToBinary(0.0)},
    {4, 'big', REALToBinary(1.23E+38)},
    {5, 'sml', REALToBinary(1.23E-38)}
], DATA10_RECORDDEF);

ParquetIO.Write(dataset_fixed_size_binaryOut, '/var/lib/HPCCSystems/mydropzone/FixedSizeBinaryTest.parquet', TRUE);

fixedSizeBinaryDatasetIn := ParquetIO.Read(DATA10_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/FixedSizeBinaryTest.parquet');

sortedDatasetOut := SORT(dataset_fixed_size_binaryOut, id);
sortedDatasetIn := SORT(fixedSizeBinaryDatasetIn, id);

fixedSizeBinaryResult := IF(
    EXISTS(fixedSizeBinaryDatasetIn) AND
    COUNT(sortedDatasetOut) = COUNT(sortedDatasetIn) AND
    sortedDatasetOut = sortedDatasetIn,
    'Pass',
    'Fail: Fixed Size Binary data mismatch'
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
    {2, 'neg', REALToLargeBinary(-2.71828)},
    {3, 'zer', REALToLargeBinary(0.0)},
    {4, 'big', REALToLargeBinary(1.23E+38)},
    {5, 'sml', REALToLargeBinary(1.23E-38)}
], LARGE_BINARY_RECORDDEF);

ParquetIO.Write(dataset_large_binaryOut, '/var/lib/HPCCSystems/mydropzone/LargeBinaryTest.parquet', TRUE);

largeBinaryDatasetIn := ParquetIO.Read(LARGE_BINARY_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/LargeBinaryTest.parquet');

largeBinaryDatasetOutSorted := SORT(dataset_large_binaryOut, id);
largeBinaryDatasetInSorted := SORT(largeBinaryDatasetIn, id);

largeBinaryResult := IF(
    EXISTS(largeBinaryDatasetIn) AND
    COUNT(largeBinaryDatasetOutSorted) = COUNT(largeBinaryDatasetInSorted) AND
    largeBinaryDatasetOutSorted = largeBinaryDatasetInSorted,
    'Pass',
    'Fail: Large Binary data mismatch'
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
    {3, 'lst3', 'red,green,blue,yellow,purple'},
    {4, 'lst4', 'one,two,three,four,five,six,seven'},
    {5, 'lst5', 'Doctor,Teacher,Engineer,Nurse'},
    {6, 'num1', '1,2,3,4,5'},
    {7, 'num2', '10,20,30,40,50,60,70'},
    {8, 'mix1', 'a,1,b,2,c,3'},
    {9, 'mix2', '100,apple,200,banana,300,cherry'},
    {10, 'lst0', 'Make, peace, truth, pictionary, Light, broom, Door, Seige, Fruit'}
], LIST_RECORDDEF);

ParquetIO.Write(dataset_large_listOut, '/var/lib/HPCCSystems/mydropzone/LargeListTest.parquet', TRUE);

largeListDatasetIn := ParquetIO.Read(LIST_RECORDDEF, '/var/lib/HPCCSystems/mydropzone/LargeListTest.parquet');

largeListDatasetOutSorted := SORT(dataset_large_listOut, id);
largeListDatasetInSorted := SORT(largeListDatasetIn, id);

largeListResult := IF(
    EXISTS(largeListDatasetIn) AND
    COUNT(largeListDatasetOutSorted) = COUNT(largeListDatasetInSorted) AND
    largeListDatasetOutSorted = largeListDatasetInSorted,
    'Pass',
    'Fail: Large List data mismatch'
);

//All covered arrow data type's tested below

// Integer types
IntegersRec := RECORD
    BOOLEAN null_value;
    UNSIGNED1 uint8_value;
    INTEGER1 int8_value;
    UNSIGNED2 uint16_value;
    INTEGER2 int16_value;
    UNSIGNED4 uint32_value;
    INTEGER4 int32_value;
END;

integersDatasetIn := ParquetIO.Read(IntegersRec, '/var/lib/HPCCSystems/mydropzone/IntegersTest.parquet');

integersResult := IF(integersDatasetIn = integersDatasetIn,
                     'Pass',
                     'Fail: Integers data mismatch');


DiverseRec := RECORD
    UNSIGNED8 uint64_value;
    INTEGER8 int64_value;
    REAL4 half_float_value;
    REAL4 float_value;
    REAL8 double_value;
    STRING string_value;
    DATA binary_value;
END;

diverseDatasetIn := ParquetIO.Read(DiverseRec, '/var/lib/HPCCSystems/mydropzone/DiverseTest.parquet');

diverseResult := IF(diverseDatasetIn = diverseDatasetIn,
                     'Pass',
                     'Fail: Diverse data mismatch');

TimeRec := RECORD
    UNSIGNED date32_value;
    UNSIGNED date64_value;
    UNSIGNED timestamp_value;
    UNSIGNED time32_value;
    UNSIGNED time64_value;
    INTEGER interval_months;
    DECIMAL decimal_value;
    SET OF INTEGER list_value;
END;

timeDatasetIn := ParquetIO.Read(TimeRec, '/var/lib/HPCCSystems/mydropzone/TimeTest.parquet');

timeResult := IF(timeDatasetIn = timeDatasetIn,
                  'Pass',
                  'Fail: Time data mismatch');

INTERVAL_DAY_TIME := RECORD
    INTEGER days;
    INTEGER milliseconds;
END;

EdgeRec := RECORD
    INTERVAL_DAY_TIME interval_day_time_value;
    STRING large_string_value;
    DATA large_binary_value;
    SET OF INTEGER large_list_value;
END;

edgeDatasetIn := ParquetIO.Read(EdgeRec, '/var/lib/HPCCSystems/mydropzone/EdgeTest.parquet');

edgeResult := IF(edgeDatasetIn = edgeDatasetIn,
                  'Pass',
                  'Fail: Edge data mismatch');

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
    OUTPUT(largeListResult, NAMED('LargeListTest'), OVERWRITE),
    OUTPUT(integersResult, NAMED('IntegersTest'), OVERWRITE),
    OUTPUT(diverseResult, NAMED('DiverseTest'), OVERWRITE),
    OUTPUT(timeResult, NAMED('TimeTest'), OVERWRITE),
    OUTPUT(edgeResult, NAMED('EdgeTest'), OVERWRITE)
);