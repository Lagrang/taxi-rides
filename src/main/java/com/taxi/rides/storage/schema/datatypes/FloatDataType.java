package com.taxi.rides.storage.schema.datatypes;

public class FloatDataType extends AbstractDataType<Float> {

  @Override
  public Float parseRawValue(String rawValue) {
    return Float.parseFloat(rawValue.trim());
  }
}
