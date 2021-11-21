package com.taxi.rides.storage.index;

import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.TreeMultimap;
import com.taxi.rides.storage.QueryPredicate;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class ColumnIndexes {

  private static final Comparator<ColumnIndex> INDEX_COMPARATOR =
      Comparator.comparingInt(i -> i.order().priotity());
  private final Multimap<String, ColumnIndex> indexes;

  public ColumnIndexes(List<ColumnIndex> indexes) {
    this.indexes = TreeMultimap.create(String::compareTo, INDEX_COMPARATOR);
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
  public Range<Long> evaluatePredicate(QueryPredicate predicate) {
    var result = Range.<Long>all();
    if (predicate.between().isEmpty()) {
      return result;
    }

    // sort indexes according to their priority and evaluate indexes with high priority first
    var betweenIndexes =
        predicate.between().stream()
            .map(
                between ->
                    this.indexes.get(between.column().name()).stream()
                        .map(index -> new IndexAndPredicate(index, i -> i.evaluateBetween(between)))
                        .iterator())
            .collect(Collectors.toList());
    var notEqualIndexes =
        predicate.notEquals().stream()
            .map(
                notEqual ->
                    this.indexes.get(notEqual.column().name()).stream()
                        .map(
                            index ->
                                new IndexAndPredicate(index, i -> i.evaluateNotEquals(notEqual)))
                        .iterator())
            .collect(Collectors.toList());
    betweenIndexes.addAll(notEqualIndexes);
    var indexes =
        Iterators.mergeSorted(
            betweenIndexes, (i1, i2) -> INDEX_COMPARATOR.compare(i1.index, i2.index));

    // here we are intersecting ranges returned by all indexes. These
    // indexes return approximate rows range. If any two index ranges do not intersect, then
    // there is no rows satisfying predicate. Intersection of all ranges returns the
    // narrowest subset of rows which can satisfy predicate.
    while (indexes.hasNext()) {
      IndexAndPredicate next = indexes.next();
      var range = next.eval().apply(next.index());
      if (!range.isConnected(result) || (result = result.intersection(range)).isEmpty()) {
        return result;
      }
    }
    return result;
  }

  record IndexAndPredicate(ColumnIndex index, Function<ColumnIndex, Range<Long>> eval) {}
}
