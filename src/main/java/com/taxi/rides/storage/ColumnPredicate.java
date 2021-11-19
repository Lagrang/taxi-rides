package com.taxi.rides.storage;

import com.taxi.rides.storage.schema.Column;
import java.util.Optional;

public final class ColumnPredicate {
  private final Between between;

  public <T extends Comparable<T>> ColumnPredicate(Between<T> between) {
    this.between = between;
  }

  public Optional<Between> between() {
    return Optional.ofNullable(between);
  }

  public record Between<T extends Comparable<T>>(Column<T> column, T from, T till) {}
}
