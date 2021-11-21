package com.taxi.rides.storage.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.Comparators;
import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate.Between;
import com.taxi.rides.storage.schema.Column;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Class maintenances sparse index of values, e.g. it records value location(e.g., row ID) for each
 * Nth value.
 */
public class SparseColumnIndex<T extends Comparable<? super T>> implements ColumnIndex<T> {

  private static final Range<Long> EMPTY_RANGE = Range.closedOpen(0L, 0L);
  private final NavigableMap<T, Long> index = new TreeMap<>();
  private T maxSeenValue;
  private final Column column;
  private final int markPeriod;
  private int leftToSkip;

  public SparseColumnIndex(Column<T> column, int markPeriod) {
    this.column = Objects.requireNonNull(column, "Index column metadata missed");
    Preconditions.checkArgument(markPeriod > 0, "Mark period should be > 0");
    this.markPeriod = markPeriod;
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
    leftToSkip--;
    maxSeenValue = maxSeenValue == null ? colValue : Comparators.max(maxSeenValue, colValue);
    if (leftToSkip <= 0) {
      leftToSkip = markPeriod;
      index.put(colValue, rowId);
    }
  }

  @Override
  public Range<Long> evaluateBetween(Between<T> predicate) {
    if (index.isEmpty()) {
      return Range.all();
    }

    // least known value is greater than predicate upper bound
    if (predicate.range().hasUpperBound()
        && index.firstEntry().getKey().compareTo(predicate.range().upperEndpoint()) > 0) {
      return EMPTY_RANGE;
    }

    // greatest known value less than predicate lower bound
    if (predicate.range().hasLowerBound()
        && maxSeenValue.compareTo(predicate.range().lowerEndpoint()) < 0) {
      return EMPTY_RANGE;
    }

    var valRange = predicate.range();
    if (valRange.hasLowerBound() && valRange.hasUpperBound()) {
      var lower = index.floorEntry(valRange.lowerEndpoint());
      if (lower == null) {
        lower = index.firstEntry();
      }
      var upper = index.ceilingEntry(valRange.upperEndpoint());
      return upper != null
          ? Range.closed(
              Math.min(lower.getValue(), upper.getValue()),
              Math.max(lower.getValue(), upper.getValue()))
          : Range.atLeast(lower.getValue());
    } else if (valRange.hasLowerBound()) {
      var lower = index.floorEntry(valRange.lowerEndpoint());
      if (lower == null) {
        lower = index.firstEntry();
      }
      return Range.atLeast(lower.getValue());
    } else if (valRange.hasUpperBound()) {
      var entry = index.ceilingEntry(valRange.upperEndpoint());
      return entry != null ? Range.atMost(entry.getValue()) : Range.all();
    } else {
      return Range.all();
    }
  }
}
