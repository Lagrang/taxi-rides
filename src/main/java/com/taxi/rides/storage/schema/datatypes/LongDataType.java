package com.taxi.rides.storage.schema.datatypes;

public final class LongDataType implements DataType<Long> {

  @Override
  public Long parseFrom(String rawValue) {
    return Long.parseLong(rawValue.trim());
  }
}