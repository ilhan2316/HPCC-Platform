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
//version compressionType='UNCOMPRESSED'
//version compressionType='Snappy'
//version compressionType='GZip'
//version compressionType='Brotli'
//version compressionType='LZ4'
//version compressionType='ZSTD'

import ^ as root;
compressionType := #IFDEFINED(root.compressionType, 'Snappy');

IMPORT Parquet;

// Define record structures
BooleanRec := RECORD UNSIGNED testid; STRING3 testname; BOOLEAN value; END;
IntegerRec := RECORD UNSIGNED testid; STRING3 testname; INTEGER value; END;
RealRec := RECORD UNSIGNED testid; STRING3 testname; REAL value; END;
DecimalRec := RECORD UNSIGNED testid; STRING3 testname; DECIMAL value; END;
StringRec := RECORD UNSIGNED testid; STRING3 testname; STRING value; END;
QStringRec := RECORD UNSIGNED testid; STRING3 testname; QSTRING value; END;
UnicodeRec := RECORD UNSIGNED testid; STRING3 testname; UNICODE value; END;
UTF8Rec := RECORD UNSIGNED testid; STRING3 testname; UTF8 value; END;
DataRec := RECORD UNSIGNED testid; STRING3 testname; DATA value; END;
VarstringRec := RECORD UNSIGNED testid; STRING3 testname; VARSTRING value; END;
VarunicodeRec := RECORD UNSIGNED testid; STRING3 testname; VARUNICODE value; END;

// Read datasets from Parquet files
BooleanData := ParquetIO.Read(BooleanRec, '/var/lib/HPCCSystems/mydropzone/Boolean.parquet');
IntegerData := ParquetIO.Read(IntegerRec, '/var/lib/HPCCSystems/mydropzone/Integer.parquet');
RealData := ParquetIO.Read(RealRec, '/var/lib/HPCCSystems/mydropzone/Real.parquet');
DecimalData := ParquetIO.Read(DecimalRec, '/var/lib/HPCCSystems/mydropzone/Decimal.parquet');
StringData := ParquetIO.Read(StringRec, '/var/lib/HPCCSystems/mydropzone/String.parquet');
QStringData := ParquetIO.Read(QStringRec, '/var/lib/HPCCSystems/mydropzone/QString.parquet');
UnicodeData := ParquetIO.Read(UnicodeRec, '/var/lib/HPCCSystems/mydropzone/Unicode.parquet');
UTF8Data := ParquetIO.Read(UTF8Rec, '/var/lib/HPCCSystems/mydropzone/UTF8.parquet');
DataData := ParquetIO.Read(DataRec, '/var/lib/HPCCSystems/mydropzone/Data.parquet');
VarstringData := ParquetIO.Read(VarstringRec, '/var/lib/HPCCSystems/mydropzone/Varstring.parquet');
VarunicodeData := ParquetIO.Read(VarunicodeRec, '/var/lib/HPCCSystems/mydropzone/Varunicode.parquet');

// Output datasets read from Parquet files
OUTPUT(BooleanData, NAMED('BooleanData'));
OUTPUT(IntegerData, NAMED('IntegerData'));
OUTPUT(RealData, NAMED('RealData'));
OUTPUT(DecimalData, NAMED('DecimalData'));
OUTPUT(StringData, NAMED('StringData'));
OUTPUT(QStringData, NAMED('QStringData'));
OUTPUT(UnicodeData, NAMED('UnicodeData'));
OUTPUT(UTF8Data, NAMED('UTF8Data'));
OUTPUT(DataData, NAMED('DataData'));
OUTPUT(VarstringData, NAMED('VarstringData'));
OUTPUT(VarunicodeData, NAMED('VarunicodeData'));
