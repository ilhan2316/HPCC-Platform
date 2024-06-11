//class=ParquetRegression

IMPORT Parquet, Std.Uni;

layout := RECORD
    INTEGER col1;
END;

smallFilePath := '/var/lib/HPCCSystems/mydropzone/small.parquet';
mediumFilePath := '/var/lib/HPCCSystems/mydropzone/medium.parquet';
largeFilePath := '/var/lib/HPCCSystems/mydropzone/large.parquet';
largestFilePath := '/var/lib/HPCCSystems/mydropzone/largest.parquet';

smallData := ParquetIO.Read(layout, smallFilePath);
mediumData := ParquetIO.Read(layout, mediumFilePath);
largeData := ParquetIO.Read(layout, largeFilePath);
largestData := ParquetIO.Read(layout, largestFilePath);

OUTPUT(smallData, NAMED('small_data'));


OUTPUT(mediumData, NAMED('medium_data'));


OUTPUT(largeData, NAMED('large_data'));


OUTPUT(largestData, NAMED('largest_data'));
