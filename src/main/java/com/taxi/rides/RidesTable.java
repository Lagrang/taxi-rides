package com.taxi.rides;

import com.google.common.collect.Range;
import com.taxi.rides.query.aggregations.DoubleAvgAggregation;
import com.taxi.rides.storage.CsvStorageFile;
import com.taxi.rides.storage.QueryPredicate;
import com.taxi.rides.storage.QueryPredicate.Between;
import com.taxi.rides.storage.Row;
import com.taxi.rides.storage.RowReader;
import com.taxi.rides.storage.index.MinMaxColumnIndex;
import com.taxi.rides.storage.index.RowLocator;
import com.taxi.rides.storage.schema.Column;
import com.taxi.rides.storage.schema.Schema;
import com.taxi.rides.storage.schema.datatypes.ByteDataType;
import com.taxi.rides.storage.schema.datatypes.FloatDataType;
import com.taxi.rides.storage.schema.datatypes.ShortDataType;
import com.taxi.rides.storage.schema.datatypes.StringDataType;
import com.taxi.rides.storage.schema.datatypes.TimestampDataType;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
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
  private final Column<Float> tripDistanceCol;
  private final List<Column> avgDistColumns;
  private List<CsvStorageFile> csvFiles;

  public RidesTable(Settings settings) {
    this.settings = settings;
    workerPool = new ForkJoinPool(settings.executionThreads);
    pickupDateCol = new Column<>("tpep_pickup_datetime", new TimestampDataType());
    dropoffDateCol = new Column<>("tpep_dropoff_datetime", new TimestampDataType());
    passengerCountCol = new Column<>("passenger_count", new ByteDataType());
    tripDistanceCol = new Column<>("trip_distance", new FloatDataType());
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
    avgDistColumns = List.of(passengerCountCol, tripDistanceCol);
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
                          .filter(Files::isRegularFile)
                          .map(
                              path ->
                                  new CsvStorageFile(
                                      path,
                                      csvSchema,
                                      new RowLocator(settings.skipIndexStep),
                                      List.of(
                                          // pickup and dropoff columns are not sorted, so we can
                                          // use only min-max index
                                          // TODO: add striped min-max
                                          new MinMaxColumnIndex<>(pickupDateCol),
                                          new MinMaxColumnIndex<>(dropoffDateCol))))
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

  @Override
  public HashMap<Integer, Double> getAverageDistances(LocalDateTime start, LocalDateTime end) {
    var predicate =
        QueryPredicate.allMatches(
            List.of(
                new Between<>(pickupDateCol, Range.atLeast(start)),
                new Between<>(dropoffDateCol, Range.atMost(end))));
    try {
      return workerPool
          .submit(
              () -> {
                var merged =
                    csvFiles.stream()
                        .parallel()
                        .map(
                            csvFile -> {
                              try {
                                return csvFile.openReader(avgDistColumns, predicate);
                              } catch (IOException e) {
                                throw new RuntimeException(e);
                              }
                            })
                        .map(this::aggregate)
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

  private HashMap<Byte, DoubleAvgAggregation> aggregate(RowReader rowReader) {
    int countIdx = rowReader.schema().getColumnIndex(passengerCountCol.name()).getAsInt();
    int distIdx = rowReader.schema().getColumnIndex(tripDistanceCol.name()).getAsInt();

    var groupby = new HashMap<Byte, DoubleAvgAggregation>();
    while (rowReader.hasNext()) {
      Row row = rowReader.next();
      groupby
          .computeIfAbsent((Byte) row.get(countIdx), key -> new DoubleAvgAggregation())
          .add((Double) row.get(distIdx));
    }
    return groupby;
  }

  @Override
  public void close() {
    csvFiles.clear();
    workerPool.shutdown();
  }

  public static class Settings {
    final int initThreads = 1;
    final int executionThreads = Runtime.getRuntime().availableProcessors();
    final int skipIndexStep = 8192;
  }
}
