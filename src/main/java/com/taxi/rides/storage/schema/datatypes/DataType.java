package com.taxi.rides.storage.schema.datatypes;

public interface DataType<T extends Comparable<T>> {

  T parseFrom(String rawValue);
}
