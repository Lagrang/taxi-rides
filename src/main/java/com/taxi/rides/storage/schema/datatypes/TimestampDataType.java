package com.taxi.rides.storage.schema.datatypes;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

public class TimestampDataType extends AbstractDataType<LocalDateTime> {

  private static final DateTimeFormatter FORMATTER =
      new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd H:m:s").toFormatter();

  @Override
  public LocalDateTime parseRawValue(String rawValue) {
    return LocalDateTime.parse(rawValue, FORMATTER);
  }
}
