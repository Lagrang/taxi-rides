package com.taxi.rides.storage.schema.datatypes;

import java.time.LocalDateTime;

public class TimestampDataType extends AbstractDataType<LocalDateTime> {

  @Override
  public LocalDateTime parseRawValue(String rawValue) {
    // parse string of format 'yyyy-MM-dd H:m:s' manually, this is more performant than
    // LocalDatetime.parse(str, FORMATTER) or pattern matching variant.
    int dtSplitIndex = rawValue.indexOf(' ');
    var date = rawValue.substring(0, dtSplitIndex);
    var time = rawValue.substring(dtSplitIndex + 1);
    int yearIdx = date.indexOf('-');
    int monthIdx = date.indexOf('-', yearIdx + 1);
    int dayIdx = monthIdx + 1;
    int hourIdx = time.indexOf(':');
    int minIdx = time.indexOf(':', hourIdx + 1);
    int secIdx = minIdx + 1;
    return LocalDateTime.of(
        Integer.parseInt(date.substring(0, yearIdx)),
        Integer.parseInt(date.substring(yearIdx + 1, monthIdx)),
        Integer.parseInt(date.substring(dayIdx)),
        Integer.parseInt(time.substring(0, hourIdx)),
        Integer.parseInt(time.substring(hourIdx + 1, minIdx)),
        Integer.parseInt(time.substring(secIdx)));
  }
}
