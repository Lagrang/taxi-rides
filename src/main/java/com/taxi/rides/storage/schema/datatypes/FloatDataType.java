package com.taxi.rides.storage.schema.datatypes;

public class FloatDataType implements DataType<Float> {

  @Override
  public Float parseFrom(String rawValue) {
    return Float.parseFloat(rawValue.trim());
  }
}
