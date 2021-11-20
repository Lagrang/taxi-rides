package com.taxi.rides.storage.schema.datatypes;

import com.google.common.base.Strings;

public abstract class AbstractDataType<T extends Comparable<? super T>> implements DataType<T> {

  @Override
  public T parseFrom(String rawValue) {
    if (Strings.isNullOrEmpty(rawValue)) {
      return null;
    }
    return parseRawValue(rawValue.trim());
  }

  /**
   * Parse data type value from raw string value.
   *
   * @param rawValue Non-null/non-empty raw value.
   * @return Parsed value.
   */
  protected abstract T parseRawValue(String rawValue);
}
