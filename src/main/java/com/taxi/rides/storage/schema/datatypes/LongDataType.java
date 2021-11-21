package com.taxi.rides.storage.schema.datatypes;

import com.google.common.primitives.Longs;

public final class LongDataType extends AbstractDataType<Long> {

  @Override
  public Long parseRawValue(String rawValue) {
    return Longs.tryParse(rawValue);
  }
}
