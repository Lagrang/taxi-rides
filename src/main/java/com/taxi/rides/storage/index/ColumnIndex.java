package com.taxi.rides.storage.index;

import com.taxi.rides.storage.schema.Column;

public interface ColumnIndex<T> {

  Column<T> column();

  void addEntry(long rowId, T value);
}
