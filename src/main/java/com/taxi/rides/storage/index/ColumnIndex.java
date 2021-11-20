package com.taxi.rides.storage.index;

import com.google.common.collect.Range;
import com.taxi.rides.storage.ColumnPredicates;
import com.taxi.rides.storage.schema.Column;

/**
 * Interface declare index on some column in dataset. This is approximate index, e.g. it returns
 * range of rows which can satisfy predicate, but not strictly follows it. Such indexes used to
 * reduce scan range inside storage files.
 *
 * @param <T>
 */
public interface ColumnIndex<T extends Comparable<T>> {

  /** Metadata for column for which this index created. */
  Column<T> column();

  /**
   * Add new index entry for row's column.
   *
   * @param rowId Row ID
   * @param colValue Column value.
   */
  void addEntry(long rowId, T colValue);

  /**
   * Evaluate passed predicate and returns range of row IDs which can satisfy to predicate
   * condition.
   *
   * @param predicate Between predicate.
   * @return
   */
  Range<Long> evaluateBetween(ColumnPredicates.Between<T> predicate);
}
