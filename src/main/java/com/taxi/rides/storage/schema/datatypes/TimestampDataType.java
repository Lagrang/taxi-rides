package com.taxi.rides.storage.schema.datatypes;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

public class TimestampDataType implements DataType<LocalDateTime> {

  private static final DateTimeFormatter FORMATTER =
      new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd hh:mm:ss").toFormatter();

  @Override
  public LocalDateTime parseFrom(String rawValue) {
    return LocalDateTime.parse(rawValue, FORMATTER);
  }
}
