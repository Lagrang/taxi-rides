package com.taxi.rides.storage.schema.datatypes;

public class ShortDataType implements DataType<Short> {

  @Override
  public Short parseFrom(String rawValue) {
    return Short.parseShort(rawValue.trim());
  }
}
