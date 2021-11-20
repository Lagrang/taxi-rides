package com.taxi.rides.storage.schema.datatypes;

public class StringDataType implements DataType<String> {

  @Override
  public String parseFrom(String rawValue) {
    return rawValue;
  }
}
