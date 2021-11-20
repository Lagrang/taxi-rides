package com.taxi.rides.storage.schema.datatypes;

public final class DoubleDataType extends AbstractDataType<Double> {

  @Override
  public Double parseRawValue(String rawValue) {
    return Double.parseDouble(rawValue.trim());
  }
}
