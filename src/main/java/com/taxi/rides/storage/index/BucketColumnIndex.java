package com.taxi.rides.storage.index;

import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate.Between;
import com.taxi.rides.storage.schema.Column;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

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
  public void addEntry(long rowId, T colValue) {
    var bucket = getBucketId.apply(colValue);
    index.compute(
        bucket,
        (key, existing) -> {
          if (existing == null) {
            return new MinMax(rowId, rowId);
          } else {
            return new MinMax(
                Math.min(existing.minRowId, rowId), Math.max(existing.maxRowId, rowId));
          }
        });
  }

  @Override
  public Range<Long> evaluateBetween(Between<T> predicate) {
    if (predicate.range().hasLowerBound() && predicate.range().hasUpperBound()) {
      var map =
          index.subMap(
              getBucketId.apply(predicate.range().lowerEndpoint()),
              true,
              getBucketId.apply(predicate.range().upperEndpoint()),
              true);
      if (map.isEmpty()) {
        return EMPTY_RANGE;
      }
      var lower = map.pollFirstEntry();
      var upper = map.pollLastEntry();
      long lowerId = lower.getValue().minRowId();
      return upper != null
          ? Range.closed(lowerId, upper.getValue().maxRowId())
          : Range.atLeast(lowerId);
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

  record MinMax(long minRowId, long maxRowId) {}
}
