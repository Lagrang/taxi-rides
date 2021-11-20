package com.taxi.rides.storage.index;

import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate.Between;
import com.taxi.rides.storage.schema.Column;

public final class MinMaxColumnIndex<T extends Comparable<? super T>> implements ColumnIndex<T> {

  private final Column<T> column;
  private T min;
  private T max;

  public MinMaxColumnIndex(Column<T> column) {
    this.column = column;
  }

  @Override
  public Column<T> column() {
    return column;
  }

  @Override
  public void addEntry(long rowId, T colValue) {
    // TODO: this is hot method during index building, try to use less branches here
    if (min == null || colValue.compareTo(min) < 0) {
      min = colValue;
    }
    if (max == null || colValue.compareTo(max) > 0) {
      max = colValue;
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
    return Range.all();
  }
}
