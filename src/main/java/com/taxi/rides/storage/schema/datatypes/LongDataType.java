package com.taxi.rides.storage.schema.datatypes;

public final class LongDataType extends AbstractDataType<Long> {

  @Override
  public Long parseRawValue(String rawValue) {
    return Long.parseLong(rawValue.trim());
  }
}
