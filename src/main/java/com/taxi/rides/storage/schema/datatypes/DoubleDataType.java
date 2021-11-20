package com.taxi.rides.storage.schema.datatypes;

public final class DoubleDataType implements DataType<Double> {

  @Override
  public Double parseFrom(String rawValue) {
    return Double.parseDouble(rawValue.trim());
  }
}
