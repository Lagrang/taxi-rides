package com.taxi.rides.storage.schema;

import com.taxi.rides.storage.schema.datatypes.DataType;

public final class Column<T extends Comparable<? super T>> {

  private final String name;
  private final DataType<T> columnType;

  public Column(String name, DataType<T> columnType) {
    this.name = name;
    this.columnType = columnType;
  }

  public String name() {
    return name;
  }

  public DataType<T> dataType() {
    return columnType;
  }
}
