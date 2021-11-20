package com.taxi.rides.storage.index;

import com.google.common.collect.Range;
import com.taxi.rides.storage.ColumnPredicates.Between;
import com.taxi.rides.storage.schema.Column;

public final class MinMaxColumnIndex<T extends Comparable<T>> implements ColumnIndex<T> {

  private final Column<T> column;
  private T min;
  private long minRow;
  private T max;
  private long maxRow;

  public MinMaxColumnIndex(Column<T> column) {
    this.column = column;
  }

  @Override
  public Column<T> column() {
    return column;
  }

  @Override
  public void addEntry(long rowId, T value) {
    // TODO: this is hot method during index building, try to use less branches here
    if (min == null || value.compareTo(min) < 0) {
      min = value;
      minRow = rowId;
    }
    if (max == null || value.compareTo(max) > 0) {
      max = value;
      maxRow = rowId;
    }
  }

  @Override
  public Range<Long> evaluateBetween(Between<T> predicate) {
    if (min == null && max == null) {
      // index is empty, we can't judge what rows satisfy predicate
      return Range.all();
    }

    var intersection = Range.closed(min, max).intersection(predicate.range());
    if (intersection.isEmpty()) {
      return Range.open(0L, 0L);
    }
    return Range.closed(minRow, maxRow);
  }
}
