package com.taxi.rides;

import com.google.common.collect.Range;
import com.taxi.rides.query.aggregations.DoubleAvgAggregation;
import com.taxi.rides.storage.CsvStorageFile;
import com.taxi.rides.storage.QueryPredicate;
import com.taxi.rides.storage.QueryPredicate.Between;
import com.taxi.rides.storage.Row;
import com.taxi.rides.storage.RowReader;
import com.taxi.rides.storage.index.BucketColumnIndex;
import com.taxi.rides.storage.index.ColumnIndex;
import com.taxi.rides.storage.index.MinMaxColumnIndex;
import com.taxi.rides.storage.index.RowOffsetLocator;
import com.taxi.rides.storage.schema.Column;
import com.taxi.rides.storage.schema.Schema;
import com.taxi.rides.storage.schema.datatypes.ByteDataType;
import com.taxi.rides.storage.schema.datatypes.DoubleDataType;
import com.taxi.rides.storage.schema.datatypes.FloatDataType;
import com.taxi.rides.storage.schema.datatypes.ShortDataType;
import com.taxi.rides.storage.schema.datatypes.StringDataType;
import com.taxi.rides.storage.schema.datatypes.TimestampDataType;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public final class RidesTable implements AverageDistances {

  private final Settings settings;
  private final Schema csvSchema;
  private final ForkJoinPool workerPool;
  private final Column<LocalDateTime> pickupDateCol;
  private final Column<LocalDateTime> dropoffDateCol;
  private final Column<Byte> passengerCountCol;
  private final Column<Double> tripDistanceCol;
  private final List<Column> avgDistColumns;
  private List<CsvStorageFile> csvFiles;

  public RidesTable(Settings settings) {
    this.settings = settings;
    workerPool = new ForkJoinPool(settings.executionThreads);
    pickupDateCol = new Column<>("tpep_pickup_datetime", new TimestampDataType());
    dropoffDateCol = new Column<>("tpep_dropoff_datetime", new TimestampDataType());
    passengerCountCol = new Column<>("passenger_count", new ByteDataType());
    tripDistanceCol = new Column<>("trip_distance", new DoubleDataType());
    csvSchema =
        new Schema(
            List.of(
                new Column<>("VendorID", new ByteDataType()),
                pickupDateCol,
                dropoffDateCol,
                passengerCountCol,
                tripDistanceCol,
                new Column<>("RatecodeID", new ByteDataType()),
                new Column<>("store_and_fwd_flag", new StringDataType()),
                new Column<>("PULocationID", new ShortDataType()),
                new Column<>("DOLocationID", new ShortDataType()),
                new Column<>("payment_type", new ByteDataType()),
                new Column<>("fare_amount", new FloatDataType()),
                new Column<>("extra", new FloatDataType()),
                new Column<>("mta_tax", new FloatDataType()),
                new Column<>("tip_amount", new FloatDataType()),
                new Column<>("tolls_amount", new FloatDataType()),
                new Column<>("improvement_surcharge", new FloatDataType()),
                new Column<>("total_amount", new FloatDataType()),
                new Column<>("congestion_surcharge", new FloatDataType())));
    avgDistColumns = List.of(pickupDateCol, dropoffDateCol, passengerCountCol, tripDistanceCol);
  }

  private static HashMap<Byte, DoubleAvgAggregation> mergeGroupbyMaps(
      HashMap<Byte, DoubleAvgAggregation> m1, HashMap<Byte, DoubleAvgAggregation> m2) {
    var res = new HashMap<>(m1);
    m2.forEach((k, v) -> res.merge(k, v, DoubleAvgAggregation::merge));
    return res;
  }

  @Override
  public void init(Path dataDir) {
    ForkJoinPool pool = new ForkJoinPool(settings.initThreads);
    try {
      csvFiles =
          pool.submit(
                  () -> {
                    try (var files = Files.walk(dataDir, FileVisitOption.FOLLOW_LINKS)) {
                      return files
                          .parallel()
                          .filter(RidesTable::isCsvFile)
                          .map(
                              path -> {
                                var fileIndexes =
                                    List.<ColumnIndex>of(
                                        new MinMaxColumnIndex<>(pickupDateCol),
                                        new MinMaxColumnIndex<>(dropoffDateCol),
                                        new BucketColumnIndex<>(
                                            pickupDateCol, t -> t.truncatedTo(ChronoUnit.DAYS)),
                                        new BucketColumnIndex<>(
                                            dropoffDateCol, t -> t.truncatedTo(ChronoUnit.DAYS)));
                                return new CsvStorageFile(
                                    path,
                                    csvSchema,
                                    new RowOffsetLocator(settings.skipIndexStep),
                                    fileIndexes);
                              })
                          .collect(Collectors.toList());
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      // simple shutdown, no need to wait termination of pool here
      pool.shutdown();
    }
  }

  private static boolean isCsvFile(Path path) {
    try {
      return Files.isRegularFile(path)
          && !Files.isHidden(path)
          && !Files.isExecutable(path)
          && path.getFileName().toString().endsWith(".csv");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HashMap<Integer, Double> getAverageDistances(LocalDateTime start, LocalDateTime end) {
    var timeRange = Range.closed(start, end);

    var predicate =
        QueryPredicate.allMatches(
            List.of(
                new Between<>(pickupDateCol, Range.atLeast(start)),
                // this safe to set more strict predicate on dropoff column, because 'dropoff'
                // always greater than 'pickup' timestamp. Such predicate change reduces query
                // time by several times(tested on taxi rides CSV files from S3).
                new Between<>(dropoffDateCol, Range.closed(start, end))));
    try {
      return workerPool
          .submit(
              () -> {
                var merged =
                    csvFiles.stream()
                        .parallel()
                        .map(csvFile -> openCsvReader(predicate, csvFile))
                        .map(rowReader -> aggregate(rowReader, timeRange))
                        .reduce(
                            new HashMap<>(),
                            RidesTable::mergeGroupbyMaps,
                            RidesTable::mergeGroupbyMaps);

                var res = new HashMap<Integer, Double>();
                merged.forEach((k, v) -> res.put((int) k, v.computeResult()));
                return res;
              })
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private RowReader openCsvReader(QueryPredicate predicate, CsvStorageFile csvFile) {
    try {
      // pass query predicate to reduce scan intervals in CSV files
      return csvFile.openReader(avgDistColumns, predicate);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private HashMap<Byte, DoubleAvgAggregation> aggregate(
      RowReader rowReader, Range<LocalDateTime> timeRange) {
    int countIdx = rowReader.schema().getColumnIndex(passengerCountCol.name()).getAsInt();
    int distIdx = rowReader.schema().getColumnIndex(tripDistanceCol.name()).getAsInt();
    int startTimeIdx = rowReader.schema().getColumnIndex(pickupDateCol.name()).getAsInt();
    int endTimeIdx = rowReader.schema().getColumnIndex(dropoffDateCol.name()).getAsInt();
    var groupby = new HashMap<Byte, DoubleAvgAggregation>();
    int count = 0;
    while (rowReader.hasNext()) {
      count++;
      Row row = rowReader.next();
      var start = (LocalDateTime) row.get(startTimeIdx);
      var end = (LocalDateTime) row.get(endTimeIdx);
      if (timeRange.contains(start) && timeRange.contains(end)) {
        var passengerCnt = (Byte) row.get(countIdx);
        var distance = (Double) row.get(distIdx);
        if (passengerCnt != null && distance != null) {
          groupby.computeIfAbsent(passengerCnt, key -> new DoubleAvgAggregation()).add(distance);
        }
      }
    }
    System.out.println("CSV read rows: " + count);
    return groupby;
  }

  @Override
  public void close() {
    csvFiles.clear();
    workerPool.shutdown();
  }

  public static class Settings {
    int initThreads = Runtime.getRuntime().availableProcessors();
    int executionThreads = Runtime.getRuntime().availableProcessors();
    int skipIndexStep = 8 * 1024;

    public Settings() {}

    public Settings(int initThreads) {
      this.initThreads = initThreads;
    }

    public Settings(int initThreads, int executionThreads) {
      this.initThreads = initThreads;
      this.executionThreads = executionThreads;
    }

    public Settings(int initThreads, int executionThreads, int skipIndexStep) {
      this.initThreads = initThreads;
      this.executionThreads = executionThreads;
      this.skipIndexStep = skipIndexStep;
    }
  }
}
