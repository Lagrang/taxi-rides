package com.taxi.rides.storage.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.taxi.rides.storage.ColumnPredicates.Between;
import com.taxi.rides.storage.schema.Column;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Class maintenances sparse index of values, e.g. it records value location(e.g., row ID) for each
 * Nth value.
 */
public class SparseColumnIndex<T extends Comparable<T>> implements ColumnIndex<T> {

  private final NavigableMap<T, Long> rowOffsets = new TreeMap<>();
  private final Column column;
  private final int markPeriod;
  private int leftToSkip;

  public SparseColumnIndex(Column column, int markPeriod) {
    this.column = Objects.requireNonNull(column, "Index column metadata missed");
    Preconditions.checkArgument(markPeriod > 0, "Mark period should be > 0");
    this.markPeriod = markPeriod;
  }

  @Override
  public Column<T> column() {
    return column;
  }

  @Override
  public void addEntry(long rowId, T colValue) {
    leftToSkip--;
    if (leftToSkip <= 0) {
      leftToSkip = markPeriod;
      rowOffsets.put(colValue, rowId);
    }
  }

  @Override
  public Range<Long> evaluateBetween(Between<T> predicate) {
    var valRange = predicate.range();
    long lower = 0;
    if (valRange.hasLowerBound()) {
      var entry = rowOffsets.floorEntry(valRange.lowerEndpoint());
      lower = entry != null ? entry.getValue() : 0;
    }

    if (valRange.hasUpperBound()) {
      var entry = rowOffsets.ceilingEntry(valRange.upperEndpoint());
      return entry != null ? Range.closed(lower, entry.getValue()) : Range.atLeast(lower);
    } else {
      return Range.atLeast(lower);
    }
  }
}
