package com.taxi.rides.storage;

import com.google.common.collect.Range;
import com.taxi.rides.storage.schema.Column;
import java.util.List;
import java.util.Objects;

public final class QueryPredicate {
  private List<Between> between = List.of();
  private List<NotEqual> notEquals = List.of();

  public QueryPredicate withBetween(List<Between> between) {
    this.between = Objects.requireNonNull(between);
    return this;
  }

  public QueryPredicate withNotEquals(List<NotEqual> notEquals) {
    this.notEquals = Objects.requireNonNull(notEquals);
    return this;
  }

  public List<Between> between() {
    return between;
  }

  public List<NotEqual> notEquals() {
    return notEquals;
  }

  public record Between<T extends Comparable<? super T>>(Column<T> column, Range<T> range) {}
  public record NotEqual<T extends Comparable<? super T>>(Column<T> column, T notEqualTo) {}
}
