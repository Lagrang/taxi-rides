package com.taxi.rides.storage.schema.datatypes;

public class ByteDataType implements DataType<Byte> {

  @Override
  public Byte parseFrom(String rawValue) {
    return Byte.parseByte(rawValue.trim());
  }
}
