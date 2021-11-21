package com.taxi.rides.storage.index;

import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate.Between;
import com.taxi.rides.storage.schema.Column;
import com.taxi.rides.storage.schema.datatypes.LongDataType;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Class maintenances sparse index of row locations inside file. It records file offset for each Nth
 * row.
 *
 * <p>This class contains methods which used to find out file offset range which corresponds to some
 * row range. Usually, these methods called with rows range computed by evaluating predicates on
 * indexes.
 */
public final class RowOffsetLocator {

  private final SparseColumnIndex<Long> index;

  public RowOffsetLocator(int markPeriod) {
    index =
        new SparseColumnIndex<>(
            new Column<>("__row_id__" + ThreadLocalRandom.current().nextLong(), new LongDataType()),
            markPeriod);
  }

  /** Add row location to index. */
  public void addEntry(long rowId, long rowOffset) {
    // inverse parameters passed to sparse index, because it builds index based on column value,
    // and we want to build index where key is row ID.
    index.addEntry(rowOffset, rowId);
  }

  /**
   * Returns file offsets which covers passed row range. Returned range is approximate: it can cover
   * more rows than requested(but never less).
   */
  public Range<Long> getClosestOffsets(Range<Long> rowRange) {
    return index.evaluateBetween(new Between<>(index.column(), rowRange));
  }
}
