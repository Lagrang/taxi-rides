# taxi-rides

## Build instructions

Requirements: JDK 17

Build and test: `./gradlew build`

Run
application: `./gradlew run --args="-f \"$CSV_FOLDER\" --from \"2020-01-01 00:00:00\" --until \"2020-05-01 00:00:00\""`

## Implementation

According defined query pattern, application tries to reduce scanning of CSV files by using column
indexes on following columns: `tpep_pickup_datetime`, `tpep_dropoff_datetime`.  
Application builds two-level index. First level is `min-max` index, it used to filter out whole file
if it not satisfies the predicate. Based on dataset located at
S3(https://s3.amazonaws.com/nyc-tlc/trip+data/), `min-max` is rarely useful, because each file
contains outlier values which enforce to scan almost all files. For such cases, when `min-max`
is nor very useful, application uses second level index, `bucket index`. Reasoning, how this index
can help, may be found in Javadoc of
the [BucketColumnIndex](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/index/BucketColumnIndex.java)
class.   
Another second level index
is [NotNullColumnIndex](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/index/NotNullColumnIndex.java)
. It helps to filter out sequential rows with NULL values at file start/end. S3 dataset sometimes
contains sequence of NULL values for `passenger_count` columns at the end of the file.

Application contains another type of
index: [SparseColumnIndex](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/index/SparseColumnIndex.java)
. It used internally by other components, but doesn't have direct application as column index,
because there is no sorted columns used by query predicate.

Column indexes provides the narrowest rows range which contains data we interested. Using this rows
range, [CsvStorageFile](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/CsvStorageFile.java)
computes start/end offsets inside CSV file. Offsets computation happens
in [RowOffsetLocator](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/index/RowOffsetLocator.java)
class.

During query evaluation, application print 'predicate push-down' statistics, e.g. how many row read
per each file, total rows in file, is file skipped, etc.

Right now, according to profiler, most of the time application spent in CSV parsing. Other code
parts is negligible against it.

During development, profiler reveals that measurable time spent to parse timestamp columns. At
beginning, parser
implementation([TimestampDataType](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/schema/datatypes/TimestampDataType.java))
use standard Java utilities to parse timestamps. These standard tools written to cover wide range of
use cases and not very performant in common case. Current version
of [TimestampDataType](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/schema/datatypes/TimestampDataType.java)
class uses observation that we have strict timestamp format which can be parsed efficiently.

## Testing

Currently, application has:

- basic tests for mostly used index types
- test which covers all parts of query execution(preparing CSV test data, initialization from CSV
  folder, query execution).

Production version should provide:

- more involved tests for all types of indexes
- tests which
  covers [CsvStorageFile](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/CsvStorageFile.java)
  class. This class populate indexes and evaluate them against query predicate.
- classes which test parsing of data types