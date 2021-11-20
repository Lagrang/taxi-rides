package com.taxi.rides.storage;

import com.google.common.collect.Range;
import com.taxi.rides.storage.schema.Column;
import java.util.Optional;

public final class ColumnPredicates {
  private final Between between;

  public <T extends Comparable<T>> ColumnPredicates(Between<T> between) {
    this.between = between;
  }

  public Optional<Between> between() {
    return Optional.ofNullable(between);
  }

  public record Between<T extends Comparable<T>>(Column<T> column, Range<T> range) {}
}
