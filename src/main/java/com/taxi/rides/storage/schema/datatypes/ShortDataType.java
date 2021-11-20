package com.taxi.rides.storage.schema.datatypes;

public class ShortDataType extends AbstractDataType<Short> {

  @Override
  public Short parseRawValue(String rawValue) {
    return Short.parseShort(rawValue.trim());
  }
}
