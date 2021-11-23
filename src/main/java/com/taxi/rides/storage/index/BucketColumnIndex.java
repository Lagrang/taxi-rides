package com.taxi.rides.storage.index;

import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate.Between;
import com.taxi.rides.storage.schema.Column;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * This type of index helps to execute queries on mostly ordered data. This means, data is not
 * strongly ordered in dataset, but ordered inside some bucket. For instance, we have a 'timestamp'
 * column with following data:
 *
 * <ol>
 *   <li>2010-12-03 01:05:00
 *   <li>2010-12-03 06:09:00
 *   <li>2010-12-03 10:30:00
 *   <li>2010-12-03 12:00:00
 *   <li>2010-12-03 07:45:00
 *   <li>2020-01-01 03:09:00
 *   <li>2020-01-01 20:00:00
 *   <li>2020-01-01 15:00:00
 *   <li>2020-01-02 14:49:00
 *   <li>2020-01-02 18:17:00
 *   <li>2020-01-02 17:00:00
 *   <li>2020-01-02 10:00:00
 * </ol>
 *
 * <p>These data is not sorted in common sense, but it sorted inside 'day bucket', e.g ., data for
 * each new day follows in order. For such data we can't use {@link SparseColumnIndex} because they
 * require data to be sorted.
 *
 * <p>{@link MinMaxColumnIndex} also not very useful in case when we use predicate 'BETWEEN
 * 2015-01-01 AND 2018-12-01'. {@link MinMaxColumnIndex} will still respond that requested data
 * range contained in dataset, but that's not true in this case.
 *
 * <p>{@link BucketColumnIndex} will help to filter out datasets which still 'selected to scan' by
 * lower cardinality indexes, such as {@link MinMaxColumnIndex}.
 */
public class BucketColumnIndex<T extends Comparable<? super T>, B extends Comparable<? super B>>
    implements ColumnIndex<T> {

  private static final Range<Long> EMPTY_RANGE = Range.closedOpen(0L, 0L);
  private final NavigableMap<B, MinMax> index = new TreeMap<>();
  private final Column<T> column;
  private final Function<T, B> getBucketId;

  public BucketColumnIndex(Column<T> column, Function<T, B> getBucketId) {
    this.column = column;
    this.getBucketId = getBucketId;
  }

  @Override
  public Column<T> column() {
    return column;
  }

  @Override
  public Priority order() {
    return Priority.LOW;
  }

  @Override
  public void addEntry(long rowId, T colValue) {
    var bucket = getBucketId.apply(colValue);
    MinMax minMax = index.computeIfAbsent(bucket, key -> new MinMax(rowId, rowId));
    minMax.minRowId = Math.min(minMax.minRowId, rowId);
    minMax.maxRowId = Math.max(minMax.maxRowId, rowId);
  }

  @Override
  public Range<Long> evaluateBetween(Between<T> predicate) {
    if (predicate.range().hasLowerBound() && predicate.range().hasUpperBound()) {
      var lowerBucket = getBucketId.apply(predicate.range().lowerEndpoint());
      var upperBucket = getBucketId.apply(predicate.range().upperEndpoint());
      var map = index.subMap(lowerBucket, true, upperBucket, true);
      if (map.isEmpty()) {
        return EMPTY_RANGE;
      }
      var lower = map.firstEntry();
      var upper = map.lastEntry();
      return upper != null
          ? Range.closed(
              Math.min(lower.getValue().minRowId, upper.getValue().minRowId),
              Math.max(lower.getValue().maxRowId, upper.getValue().maxRowId))
          : Range.closed(lower.getValue().minRowId, lower.getValue().maxRowId);
    } else if (predicate.range().hasLowerBound()) {
      var map = index.tailMap(getBucketId.apply(predicate.range().lowerEndpoint()));
      if (map.isEmpty()) {
        return EMPTY_RANGE;
      }
      var lower =
          map.values().stream()
              .reduce(0L, (minRowId, minMax) -> Math.min(minRowId, minMax.minRowId), Math::min);
      return Range.atLeast(lower);
    } else if (predicate.range().hasUpperBound()) {
      var map = index.headMap(getBucketId.apply(predicate.range().upperEndpoint()), true);
      if (map.isEmpty()) {
        return EMPTY_RANGE;
      }
      var upper =
          map.values().stream()
              .reduce(0L, (maxRowId, minMax) -> Math.max(maxRowId, minMax.maxRowId), Math::max);
      return Range.atMost(upper);
    } else {
      return Range.all();
    }
  }

  class MinMax {
    long minRowId;
    long maxRowId;

    public MinMax(long minRowId, long maxRowId) {
      this.minRowId = minRowId;
      this.maxRowId = maxRowId;
    }
  }
}
