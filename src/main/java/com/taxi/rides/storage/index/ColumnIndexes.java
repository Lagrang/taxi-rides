package com.taxi.rides.storage.index;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate;
import com.taxi.rides.storage.QueryPredicate.Between;
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
  public Range<Long> evaluate(QueryPredicate predicate) {
    var result = Range.<Long>all();
    if (predicate.between().isEmpty()) {
      return result;
    }

    for (Between between : predicate.between()) {
      var indexes = this.indexes.get(between.column().name());
      // here we are intersecting ranges returned by several indexes for the same column. These
      // indexes return approximate rows range and if any two index ranges do not intersect, then
      // there is no rows satisfying predicate. Intersection of all ranges returns known the
      // narrowest subset of rows which can satisfy predicate.
      for (ColumnIndex index : indexes) {
        var range = index.evaluateBetween(between);
        result = result.intersection(range);
        if (result.isEmpty()) {
          return result;
        }
      }
    }
    return result;
  }
}
