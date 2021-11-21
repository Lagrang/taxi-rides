package com.taxi.rides.storage.index;

import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate.NotEqual;
import com.taxi.rides.storage.schema.Column;

/** Index maintenance rows range which contains non-null values. */
public class NotNullColumnIndex<T extends Comparable<? super T>> implements ColumnIndex<T> {

  private static final int UNDEFINED = -1;

  private final Column<T> column;
  private long minRow = Long.MAX_VALUE;
  private long maxRow = UNDEFINED;
  private long lastSeenNullRow = UNDEFINED;

  public NotNullColumnIndex(Column<T> column) {
    this.column = column;
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
    if (colValue != null) {
      minRow = Math.min(rowId, minRow);
      maxRow = Math.max(rowId, maxRow);
    } else {
      lastSeenNullRow = rowId;
    }
  }

  @Override
  public Range<Long> evaluateNotEquals(NotEqual<T> predicate) {
    if (predicate.notEqualTo() == null) {
      if (maxRow == UNDEFINED) {
        // index is empty and can't estimate which rows to return
        // or
        // we saw only null values
        return lastSeenNullRow == UNDEFINED ? Range.all() : Range.greaterThan(lastSeenNullRow);
      }
      return Range.closed(minRow, maxRow);
    } else {
      // predicate defined on non-NULL value
      return Range.all();
    }
  }
}
