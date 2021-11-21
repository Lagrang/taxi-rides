package com.taxi.rides.storage.schema.datatypes;

import java.time.LocalDateTime;

public class TimestampDataType extends AbstractDataType<LocalDateTime> {

  @Override
  public LocalDateTime parseRawValue(String rawValue) {
    // parse string of format 'yyyy-MM-dd H:m:s' manually, this is more performant than
    // LocalDatetime.parse(str, FORMATTER) or pattern matching variant.
    return LocalDateTime.of(
        Integer.parseInt(rawValue.substring(0, 4)),
        Integer.parseInt(rawValue.substring(5, 7)),
        Integer.parseInt(rawValue.substring(8, 10)),
        Integer.parseInt(rawValue.substring(11, 13)),
        Integer.parseInt(rawValue.substring(14, 16)),
        Integer.parseInt(rawValue.substring(17)));
  }
}
