package com.taxi.rides.storage;

/**
 * Row contains column values at indexes defined by {@link com.taxi.rides.storage.schema.Schema}.
 */
public final class Row {

  private final Object[] colValues;

  public Row(int columnCount) {
    this.colValues = new Object[columnCount];
  }

  public void set(int colIndex, Object value) {
    colValues[colIndex] = value;
  }

  public Object get(int colIndex) {
    return colValues[colIndex];
  }
}
