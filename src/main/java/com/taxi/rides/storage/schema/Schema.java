package com.taxi.rides.storage.schema;

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public final class Schema {

  private final List<Column> columns;

  public Schema(List<Column> columns) {
    this.columns = List.copyOf(columns);
  }

  public List<Column> columns() {
    return columns;
  }

  public Column getColumnAt(int index) {
    return columns.get(index);
  }

  public OptionalInt getColumnIndex(String colName) {
    var index = Iterables.indexOf(columns, column -> colName.equals(column.name()));
    return index >= 0 ? OptionalInt.of(index) : OptionalInt.empty();
  }

  public Optional<Column> getColumn(String colName) {
    for (Column column : columns) {
      if (column.name().equals(colName)) {
        return Optional.of(column);
      }
    }
    return Optional.empty();
  }
}
