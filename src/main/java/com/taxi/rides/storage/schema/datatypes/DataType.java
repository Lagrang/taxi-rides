package com.taxi.rides.storage.schema.datatypes;

public interface DataType<T> {

  T parseFrom(String rawValue);
}
