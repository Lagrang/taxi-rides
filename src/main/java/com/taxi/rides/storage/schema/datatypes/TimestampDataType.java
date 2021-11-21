package com.taxi.rides.storage.schema.datatypes;

import java.time.LocalDateTime;

public class TimestampDataType extends AbstractDataType<LocalDateTime> {

  @Override
  public LocalDateTime parseRawValue(String rawValue) {
    // parse string of format 'yyyy-MM-dd HH:mm:ss' manually, this is more performant than
    // LocalDatetime.parse(str, FORMATTER).
    return LocalDateTime.of(
        parseInt(rawValue, 0, 4),
        parseInt(rawValue, 5, 7),
        parseInt(rawValue, 8, 10),
        parseInt(rawValue, 11, 13),
        parseInt(rawValue, 14, 16),
        parseInt(rawValue, 17, rawValue.length()));
  }

  static int parseInt(String s, int beginIndex, int endIndex) {
    int sum = 0;
    int multiplier = 1;
    for (int i = endIndex - 1; i >= beginIndex; i--) {
      sum += (s.charAt(i) - '0') * multiplier;
      multiplier *= 10;
    }
    return sum;
  }
}
