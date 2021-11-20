package com.taxi.rides.storage.schema.datatypes;

public class ByteDataType extends AbstractDataType<Byte> {

  @Override
  public Byte parseRawValue(String rawValue) {
    return Byte.parseByte(rawValue.trim());
  }
}
