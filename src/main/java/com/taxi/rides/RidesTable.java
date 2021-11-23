package com.taxi.rides;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Range;
import com.taxi.rides.query.aggregations.DoubleAvgAggregation;
import com.taxi.rides.storage.CsvStorageFile;
import com.taxi.rides.storage.QueryPredicate;
import com.taxi.rides.storage.QueryPredicate.Between;
import com.taxi.rides.storage.QueryPredicate.NotEqual;
import com.taxi.rides.storage.Row;
import com.taxi.rides.storage.RowReader;
import com.taxi.rides.storage.index.BucketColumnIndex;
import com.taxi.rides.storage.index.ColumnIndex;
import com.taxi.rides.storage.index.MinMaxColumnIndex;
import com.taxi.rides.storage.index.NotNullColumnIndex;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    if (settings.disableMinMaxIndex) {
      System.out.println("Min-max index disabled");
    }
    if (settings.disableNotNullIndex) {
      System.out.println("Not-null index disabled");
    }
    if (settings.disableBucketIndex) {
      System.out.println("Bucket index disabled");
    }
  }

  private static HashMap<Byte, DoubleAvgAggregation> mergeGroupbyMaps(
      HashMap<Byte, DoubleAvgAggregation> m1, HashMap<Byte, DoubleAvgAggregation> m2) {
    var res = new HashMap<>(m1);
    m2.forEach((k, v) -> res.merge(k, v, DoubleAvgAggregation::merge));
    return res;
  }

  @Override
  public void init(Path dataDir) {
    if (!Files.exists(dataDir)) {
      throw new IllegalArgumentException(dataDir + " is not exists");
    }

    ForkJoinPool pool = new ForkJoinPool(settings.initThreads);
    try {
      csvFiles =
          pool.submit(
                  () -> {
                    try (var files = Files.walk(dataDir, FileVisitOption.FOLLOW_LINKS)) {
                      return files
                          .parallel()
                          .filter(RidesTable::isCsvFile)
                          .flatMap(this::openCsvFile)
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

  /** Open CSV file at path and logically split it to several files if needed. */
  private Stream<CsvStorageFile> openCsvFile(Path path) {
    var res = new ArrayList<CsvStorageFile>();
    long startAt = 0;
    long fileSize;
    try {
      fileSize = Files.size(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    while (true) {
      var file =
          new CsvStorageFile(
              path,
              csvSchema,
              new RowOffsetLocator(settings.skipIndexStep),
              prepareIndexes(),
              startAt,
              settings.splitSize);
      res.add(file);
      if (fileSize <= file.endOffset() + 1) {
        break;
      }
      startAt = file.endOffset() + 1;
    }
    return res.stream();
  }

  private List<ColumnIndex> prepareIndexes() {
    var indexList = new ArrayList<ColumnIndex>();
    if (!settings.disableMinMaxIndex) {
      indexList.add(new MinMaxColumnIndex<>(pickupDateCol));
      indexList.add(new MinMaxColumnIndex<>(dropoffDateCol));
    }
    if (!settings.disableBucketIndex) {
      indexList.add(new BucketColumnIndex<>(pickupDateCol, t -> t.truncatedTo(ChronoUnit.DAYS)));
      indexList.add(new BucketColumnIndex<>(dropoffDateCol, t -> t.truncatedTo(ChronoUnit.DAYS)));
    }
    if (!settings.disableNotNullIndex) {
      indexList.add(new NotNullColumnIndex<>(passengerCountCol));
      indexList.add(new NotNullColumnIndex<>(tripDistanceCol));
    }
    return indexList;
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
        new QueryPredicate()
            .withBetween(
                List.of(
                    new Between<>(pickupDateCol, Range.atLeast(start)),
                    // this safe to set more strict predicate on dropoff column, because 'dropoff'
                    // always greater than 'pickup' timestamp. Such predicate change reduces query
                    // time by several times(tested on taxi rides CSV files from S3).
                    new Between<>(dropoffDateCol, Range.closed(start, end))))
            .withNotEquals(
                List.of(
                    new NotEqual(passengerCountCol, null), new NotEqual(tripDistanceCol, null)));
    try {
      // scan each CSV in separate thread
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
    long totalMs = 0;
    var sw = Stopwatch.createUnstarted();
    try (var usedToCloseReader = rowReader) {
      while (rowReader.hasNext()) {
        Row row = rowReader.next();
        sw.start();
        var start = (LocalDateTime) row.get(startTimeIdx);
        var end = (LocalDateTime) row.get(endTimeIdx);
        if (start != null && end != null && timeRange.contains(start) && timeRange.contains(end)) {
          var passengerCnt = (Byte) row.get(countIdx);
          var distance = (Double) row.get(distIdx);
          if (passengerCnt != null && distance != null) {
            groupby.computeIfAbsent(passengerCnt, key -> new DoubleAvgAggregation()).add(distance);
          }
        }
        sw.stop();
        totalMs += sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
      }

      if (totalMs > 300) {
        System.out.println("Agg time took " + totalMs + "ms");
      }
      rowReader.printStats();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

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
    long splitSize = 100 * 1024 * 1024;
    boolean disableBucketIndex = false;
    boolean disableNotNullIndex = false;
    boolean disableMinMaxIndex = false;

    public Settings() {}

    public Settings(long splitSize) {
      this.splitSize = splitSize;
    }

    public Settings(
        int initThreads,
        int executionThreads,
        int skipIndexStep,
        long splitSize,
        boolean disableBucketIndex,
        boolean disableNotNullIndex,
        boolean disableMinMaxIndex) {
      this.initThreads = initThreads;
      this.executionThreads = executionThreads;
      this.skipIndexStep = skipIndexStep;
      this.splitSize = splitSize;
      this.disableBucketIndex = disableBucketIndex;
      this.disableNotNullIndex = disableNotNullIndex;
      this.disableMinMaxIndex = disableMinMaxIndex;
    }
  }
}
