package com.taxi.rides.storage;

import com.google.common.collect.Range;
import com.taxi.rides.storage.schema.Column;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class QueryPredicate {
  private final List<Between> between;

  private QueryPredicate(List<Between> between) {
    this.between = Collections.unmodifiableList(new ArrayList<>(between));
  }

  public static QueryPredicate allMatches(List<Between> between) {
    return new QueryPredicate(between);
  }

  public List<Between> between() {
    return between;
  }

  public record Between<T extends Comparable<? super T>>(Column<T> column, Range<T> range) {}
}
