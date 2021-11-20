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

    // here we are intersecting ranges returned by all indexes. These
    // indexes return approximate rows range. If any two index ranges do not intersect, then
    // there is no rows satisfying predicate. Intersection of all ranges returns the
    // narrowest subset of rows which can satisfy predicate.
    for (Between between : predicate.between()) {
      var indexes = this.indexes.get(between.column().name());
      for (ColumnIndex index : indexes) {
        var range = index.evaluateBetween(between);
        if (!range.isConnected(result) || (result = result.intersection(range)).isEmpty()) {
          return result;
        }
      }
    }
    return result;
  }
}
