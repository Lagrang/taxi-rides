# taxi-rides

## Build instructions

Requirements: JDK 17

Build and test: `./gradlew build`

Run
application: `./gradlew run --args="-f \"$CSV_FOLDER\" --from \"2020-05-01 00:00:00\" --until \"2020-08-11 00:00:00\""`

Print help: `./gradlew run --args="-h"`

#### Example run:

```
./gradlew run --args="-f \"/home/lagrang/Downloads/trip_data\" --from \"2020-05-01 00:00:00\" --until \"2020-08-11 00:00:00\" --query-threads=8 --init-threads=8 --split-size=70"

Initializing from folder: /home/lagrang/Downloads/trip_data
Initialization took 8sec

yellow_tripdata_2020-02.csv(146800935:220201371): skipped using indexes.
yellow_tripdata_2020-10.csv(146800841:154917591): skipped using indexes.
yellow_tripdata_2020-02.csv(73400496:146800934): skipped using indexes.
yellow_tripdata_2020-02.csv(293601790:367002258): skipped using indexes.
yellow_tripdata_2020-02.csv(367002259:440402748): skipped using indexes.
yellow_tripdata_2020-01.csv(73400501:146801001): skipped using indexes.
yellow_tripdata_2020-01.csv(146801002:220201419): skipped using indexes.
yellow_tripdata_2020-11.csv(73400487:138989554): 319489 rows read/total rows=714299.
yellow_tripdata_2020-08.csv(0:73400490): 507905 rows read/total rows=798072.
yellow_tripdata_2020-09.csv(73400440:123394594): 155649 rows read/total rows=544910.
yellow_tripdata_2020-10.csv(73400412:146800840): 532481 rows read/total rows=794966.
yellow_tripdata_2020-01.csv(293601878:367002304): skipped using indexes.
yellow_tripdata_2020-01.csv(220201420:293601877): skipped using indexes.
yellow_tripdata_2020-01.csv(513803219:587203695): skipped using indexes.
yellow_tripdata_2020-01.csv(440402717:513803218): skipped using indexes.
yellow_tripdata_2020-10.csv(0:73400411): 761857 rows read/total rows=794832.
yellow_tripdata_2020-07.csv(0:73326706): 745473 rows read/total rows=800412.
yellow_tripdata_2020-11.csv(0:73400486): 704513 rows read/total rows=794686.
yellow_tripdata_2020-01.csv(0:73400500): 793962 rows read/total rows=793962.
yellow_tripdata_2020-02.csv(0:73400495): skipped using indexes.
yellow_tripdata_2020-01.csv(587203696:593610735): skipped using indexes.
yellow_tripdata_2020-08.csv(73400491:92411544): 139265 rows read/total rows=209212.
yellow_tripdata_2020-09.csv(0:73400439): 606209 rows read/total rows=796102.
yellow_tripdata_2020-02.csv(220201372:293601789): 791038 rows read/total rows=791038.
yellow_tripdata_2020-03.csv(73400504:146800992): skipped using indexes.
yellow_tripdata_2020-02.csv(440402749:513803221): skipped using indexes.
yellow_tripdata_2020-04.csv(0:21662260): skipped using indexes.
yellow_tripdata_2020-05.csv(0:31641589): 294913 rows read/total rows=348371.
yellow_tripdata_2020-06.csv(0:50277192): 499713 rows read/total rows=549760.
yellow_tripdata_2020-03.csv(220201482:278288607): 598017 rows read/total rows=633537.
yellow_tripdata_2020-12.csv(73400444:134481399): 368641 rows read/total rows=666682.
yellow_tripdata_2020-01.csv(367002305:440402716): 790805 rows read/total rows=790805.
yellow_tripdata_2020-02.csv(513803222:584190584): 712705 rows read/total rows=760823.
yellow_tripdata_2020-12.csv(0:73400443): 573441 rows read/total rows=795215.
yellow_tripdata_2020-03.csv(0:73400503): 791239 rows read/total rows=791239.
yellow_tripdata_2020-03.csv(146800993:220201481): 791524 rows read/total rows=791524.
Query took: 2sec

Average distances(passengers count to average distance):
0 : 2.633296195986711
1 : 2.721228644785086
2 : 2.940584374970827
3 : 2.8780335840741578
4 : 3.033393323039244
5 : 2.812991578535052
6 : 2.8505635911717975
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