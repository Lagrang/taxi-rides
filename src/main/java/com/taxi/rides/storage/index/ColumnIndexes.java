package com.taxi.rides.storage.index;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.taxi.rides.storage.ColumnPredicates;
import java.util.List;

public final class ColumnIndexes {

  private final Multimap<String, ColumnIndex> indexes;

  public ColumnIndexes(List<ColumnIndex> indexes) {
    // usually we have 1 index per column
    this.indexes = HashMultimap.create(indexes.size(), 1);
    for (ColumnIndex index : indexes) {
      this.indexes.put(index.column().name(), index);
    }
  }

  /**
   * Compute range of rows which falls under predicate condition.
   *
   * @param predicate Column predicate
   * @return Range of row's IDs.
   */
  public Range<Long> evaluate(ColumnPredicates predicate) {
    // currently, only 1 predicate type supported
    if (predicate.between().isEmpty()) {
      return Range.all();
    }

    var between = predicate.between().get();
    var indexes = this.indexes.get(between.column().name());

    // here we are intersecting ranges returned by several indexes for the same column. These
    // indexes return approximate rows range and if any two index ranges do not intersect, then
    // there is no rows satisfying predicate. Intersection of all ranges returns the narrowest
    // subset of rows which can satisfy predicate.
    return indexes.stream()
        .map(columnIndex -> columnIndex.evaluateBetween(between))
        .reduce(Range.all(), Range::intersection, Range::intersection);
  }
}
