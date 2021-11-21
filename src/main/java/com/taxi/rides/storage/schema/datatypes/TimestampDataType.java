package com.taxi.rides.storage.schema.datatypes;

import java.time.LocalDateTime;

public class TimestampDataType extends AbstractDataType<LocalDateTime> {

  @Override
  public LocalDateTime parseRawValue(String rawValue) {
    // parse string of format 'yyyy-MM-dd H:m:s' manually, this is more performant than
    // LocalDatetime.parse(str, FORMATTER) or regex.
    return LocalDateTime.of(
        Integer.parseInt(rawValue, 0, 4, 10),
        Integer.parseInt(rawValue, 5, 7, 10),
        Integer.parseInt(rawValue, 8, 10, 10),
        Integer.parseInt(rawValue, 11, 13, 10),
        Integer.parseInt(rawValue, 14, 16, 10),
        Integer.parseInt(rawValue, 17, rawValue.length(), 10));
  }
}
