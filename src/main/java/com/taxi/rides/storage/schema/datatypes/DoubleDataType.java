package com.taxi.rides.storage.schema.datatypes;

import com.taxi.rides.storage.schema.datatypes.fastdoubleparser.FastDoubleParser;

public final class DoubleDataType extends AbstractDataType<Double> {

  @Override
  public Double parseRawValue(String rawValue) {
    return FastDoubleParser.parseDouble(rawValue.trim());
  }
}
