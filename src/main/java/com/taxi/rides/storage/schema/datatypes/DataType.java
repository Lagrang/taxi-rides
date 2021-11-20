package com.taxi.rides.storage.schema.datatypes;

public interface DataType<T extends Comparable<? super T>> {

  T parseFrom(String rawValue);
}
