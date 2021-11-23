# taxi-rides

## Build instructions

Requirements: JDK 17

Build and test: `./gradlew build`

Run
application: `./gradlew run --args="-f \"$CSV_FOLDER\" --from \"2020-05-01 00:00:00\" --until \"2020-08-11 00:00:00\""`

Print help: `./gradlew run --args="-h"`

#### Example run:

```
./gradlew run --args="-f \"/home/lagrang/Downloads/trip_data\" --from \"2020-05-01 00:00:00\" --until \"2020-08-11 00:00:00\" --query-threads=4 --split-size=130"
Initializing from folder: /home/lagrang/Downloads/trip_data
Initialization took 25sec

yellow_tripdata_2020-01.csv(0:136315006): skipped using indexes.
yellow_tripdata_2020-11.csv(136314980:138989554): skipped using indexes.
yellow_tripdata_2020-01.csv(136315007:272630053): skipped using indexes.
yellow_tripdata_2020-09.csv(0:123394594): skipped using indexes.
yellow_tripdata_2020-10.csv(136314992:154917591): skipped using indexes.
yellow_tripdata_2020-10.csv(0:136314991): skipped using indexes.
yellow_tripdata_2020-01.csv(408945054:545260123): skipped using indexes.
yellow_tripdata_2020-01.csv(545260124:593610735): skipped using indexes.
yellow_tripdata_2020-02.csv(136315010:272630047): 32769 rows read/total rows=1468771.
yellow_tripdata_2020-02.csv(408945054:545260107): skipped using indexes.
yellow_tripdata_2020-02.csv(272630048:408945053): skipped using indexes.
yellow_tripdata_2020-01.csv(272630054:408945053): 57345 rows read/total rows=1470477.
yellow_tripdata_2020-11.csv(0:136314979): skipped using indexes.
yellow_tripdata_2020-03.csv(272630049:278288607): skipped using indexes.
yellow_tripdata_2020-12.csv(0:134481399): skipped using indexes.
yellow_tripdata_2020-04.csv(0:21662260): skipped using indexes.
yellow_tripdata_2020-03.csv(0:136315002): 16385 rows read/total rows=1469303.
yellow_tripdata_2020-03.csv(136315003:272630048): 139265 rows read/total rows=1473848.
yellow_tripdata_2020-02.csv(545260108:584190584): 16385 rows read/total rows=421874.
yellow_tripdata_2020-05.csv(0:31641589): 294913 rows read/total rows=348371.
yellow_tripdata_2020-02.csv(0:136315009): skipped using indexes.
yellow_tripdata_2020-06.csv(0:50277192): 499713 rows read/total rows=549760.
yellow_tripdata_2020-07.csv(0:73326706): 745473 rows read/total rows=800412.
yellow_tripdata_2020-08.csv(0:92411544): 942081 rows read/total rows=1007284.
Query took: 1sec

Average distances(passengers count to average distance):
0 : 2.633296195986711
1 : 2.721230739382146
2 : 2.940584374970827
3 : 2.8780335840741578
4 : 3.033393323039244
5 : 2.812991578535052
6 : 2.8506969361166803
7 : 2.168
8 : 0.0
9 : 0.0


```

## Implementation

Base idea is to use CSV based storage 'as is', without converting to some other format. Application
tries to reduce scanning of CSV files by using column indexes on following
columns: `tpep_pickup_datetime`, `tpep_dropoff_datetime`.  
Application builds two-level index. First level is `min-max` index, it is used to filter out whole
file if it is not satisfies the predicate. Based on dataset located at
S3(https://s3.amazonaws.com/nyc-tlc/trip+data/), `min-max` is rarely useful, because each file
contains outlier values which enforce to scan almost all files. For such cases, when `min-max`
is not very useful, application uses second level index, `bucket index`. More information about this
index may be found in Javadoc of
the [BucketColumnIndex](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/index/BucketColumnIndex.java)
class.   
Another second level index
is [NotNullColumnIndex](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/index/NotNullColumnIndex.java)
. It helps to filter out sequential rows with NULL values at file start/end. S3 dataset sometimes
contains sequence of NULL values for `passenger_count` columns at the end of the file.   
Affect of each index type can be measured by disabling each of them(through command line arguments).

Application contains another type of
index: [SparseColumnIndex](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/index/SparseColumnIndex.java)
. It is used internally by other components, but doesn't have direct application as column index,
because there is no sorted columns used by query predicate.

Column indexes provides the narrowest rows range which contains data we interested in. Using this
rows
range, [CsvStorageFile](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/CsvStorageFile.java)
computes start/end offsets inside CSV file. Offsets computation happens
in [RowOffsetLocator](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/index/RowOffsetLocator.java)
class.

Second optimization is CSV file splitting. If size of CSV file greater than defined 'split point'
(though command line), then it will be logically split into several files. Each file will be
processed by different thread. Each logical file will have it own indexes.

During query evaluation, application print 'predicate push-down' statistics, e.g. how many row read
per each file, total rows in file, is file skipped, etc.

Right now, according to profiler, most of the time application spent in CSV parsing. Other code
parts is negligible against it.

During development, profiler reveals that measurable time spent to parse timestamp columns. At
beginning, parser
implementation([TimestampDataType](https://github.com/Lagrang/taxi-rides/blob/main/src/main/java/com/taxi/rides/storage/schema/datatypes/TimestampDataType.java))
uses standard Java utilities to parse timestamps. These standard tools are written to cover wide
range of use cases and not very performant in common case. Current version
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