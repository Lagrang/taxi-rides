package com.taxi.rides.storage.index;

import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate;
import com.taxi.rides.storage.schema.Column;

/**
 * Interface declare index on some column in dataset. This is approximate index, e.g. it returns
 * range of rows which can satisfy predicate, but not strictly follows it. Such indexes used to
 * reduce scan range inside storage files.
 *
 * @param <T>
 */
public interface ColumnIndex<T extends Comparable<? super T>> {

  /** Metadata for column for which this index created. */
  Column<T> column();

  /**
   * Return value which defines 'priority' of this index. Index with less priority ({@link
   * Priority#priotity()}) value should be evaluated first. This will ensure that indexes with less
   * granularity(for instance, min-max index) will be examined first and let to skip file without
   * checking more involved indexes for same file.
   */
  Priority order();

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
  Range<Long> evaluateBetween(QueryPredicate.Between<T> predicate);

  enum Priority {
    HIGH(0),
    LOW(1),
    ;

    private final int priotity;

    Priority(int priotity) {
      this.priotity = priotity;
    }

    public int priotity() {
      return priotity;
    }
  }
}
