package com.taxi.rides.storage.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;

import com.google.common.collect.Comparators;
import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate.Between;
import com.taxi.rides.storage.schema.Column;
import com.taxi.rides.storage.schema.datatypes.TimestampDataType;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.RepeatedTest;

public class BucketColumnIndexTest {

  public static final int REPEAT = 1000;

  @RepeatedTest(REPEAT)
  void testOnSortedBuckets() {
    var col = new Column<>("col", new TimestampDataType());
    var index =
        new BucketColumnIndex<>(
            col,
            val ->
                LocalDateTime.from(
                    TemporalAdjusters.firstDayOfMonth()
                        .adjustInto(val.truncatedTo(ChronoUnit.DAYS))));

    LocalDateTime min = LocalDateTime.MAX;
    LocalDateTime max = LocalDateTime.MIN;
    long maxRowId = -1;
    int month1 = ThreadLocalRandom.current().nextInt(1, 13);
    int month2 = ThreadLocalRandom.current().nextInt(1, 13);
    var monthRangeToCheck =
        Range.closed(
            LocalDateTime.of(2020, Math.min(month1, month2), 1, 0, 0),
            LocalDateTime.of(2020, Math.max(month1, month2), 1, 0, 0));
    long expectedStartRow = Long.MAX_VALUE;
    long expectedEndRow = 0;
    // generate data for 12 months, to create 12 buckets in sorted order
    for (int i = 1; i <= 12; i++) {
      // each month contains 30 records with random day number(not sorted)
      if (i >= monthRangeToCheck.lowerEndpoint().getMonthValue()
          && i <= monthRangeToCheck.upperEndpoint().getMonthValue()) {
        expectedStartRow = Math.min(expectedStartRow, maxRowId + 1);
        expectedEndRow = Math.max(expectedEndRow, maxRowId + 30);
      }
      for (int days = 0; days < 30; days++) {
        var value =
            LocalDateTime.of(
                2020,
                i,
                ThreadLocalRandom.current().nextInt(1, 29),
                ThreadLocalRandom.current().nextInt(0, 24),
                ThreadLocalRandom.current().nextInt(0, 60));
        maxRowId++;
        index.addEntry(maxRowId, value);
        min = Comparators.min(min, value);
        max = Comparators.max(max, value);
      }
    }

    var range = index.evaluateBetween(new Between<>(col, Range.closed(min, max)));
    assertThat(range.lowerEndpoint()).isEqualTo(0);
    assertThat(range.upperEndpoint()).isEqualTo(maxRowId);

    range = index.evaluateBetween(new Between<>(col, Range.atLeast(min)));
    assertThat(range.lowerEndpoint()).isEqualTo(0);
    long finalMaxRowId = maxRowId;
    assertThatObject(range).matches(r -> !r.hasUpperBound() || r.upperEndpoint() == finalMaxRowId);

    range = index.evaluateBetween(new Between<>(col, Range.atMost(max)));
    assertThatObject(range)
        .matches(
            r ->
                (!r.hasLowerBound() || r.lowerEndpoint() == 0)
                    && (r.upperEndpoint() == finalMaxRowId));

    range = index.evaluateBetween(new Between<>(col, monthRangeToCheck));
    assertThat(range.lowerEndpoint()).isEqualTo(expectedStartRow);
    assertThat(range.upperEndpoint()).isEqualTo(expectedEndRow);
  }

  @RepeatedTest(REPEAT)
  void testOnUnsortedBuckets() {
    var col = new Column<>("col", new TimestampDataType());
    var index =
        new BucketColumnIndex<>(
            col,
            val ->
                LocalDateTime.from(
                    TemporalAdjusters.firstDayOfMonth()
                        .adjustInto(val.truncatedTo(ChronoUnit.DAYS))));

    LocalDateTime min = LocalDateTime.MAX;
    LocalDateTime max = LocalDateTime.MIN;
    long minValRowId = 0;
    long maxValRowId = 0;
    long nextRowId = -1;
    // generate data for 12 months, to create 12 buckets in without order
    for (int i = 0; i <= 1000; i++) {
      var value =
          LocalDateTime.of(
              2020,
              ThreadLocalRandom.current().nextInt(1, 13),
              ThreadLocalRandom.current().nextInt(1, 29),
              ThreadLocalRandom.current().nextInt(0, 24),
              ThreadLocalRandom.current().nextInt(0, 60));
      nextRowId++;
      index.addEntry(nextRowId, value);
      min = Comparators.min(min, value);
      if (min.equals(value)) {
        minValRowId = nextRowId;
      }
      max = Comparators.max(max, value);
      if (max.equals(value)) {
        maxValRowId = nextRowId;
      }
    }

    var range = index.evaluateBetween(new Between<>(col, Range.closed(min, max)));
    var expected =
        Range.closed(Math.min(minValRowId, maxValRowId), Math.max(maxValRowId, minValRowId));
    if (!range.encloses(expected)) {
      index.evaluateBetween(new Between<>(col, Range.closed(min, max)));
    }
    assertThat(range.encloses(expected))
        .withFailMessage("Expected " + expected + ", actual " + range)
        .isTrue();
  }

  @RepeatedTest(REPEAT)
  void testOutsideRange() {
    var col = new Column<>("col", new TimestampDataType());
    var index =
        new BucketColumnIndex<>(
            col,
            val ->
                LocalDateTime.from(
                    TemporalAdjusters.firstDayOfMonth()
                        .adjustInto(val.truncatedTo(ChronoUnit.DAYS))));

    var start = LocalDateTime.now();
    var end = start;
    for (int i = 0; i < REPEAT; i++) {
      index.addEntry(1, end);
      end = end.plusMonths(i);
    }

    assertThat(
            List.of(
                index.evaluateBetween(new Between<>(col, Range.atMost(start.minusMonths(1)))),
                index.evaluateBetween(
                    new Between<>(col, Range.closedOpen(start.minusMonths(1), start))),
                index.evaluateBetween(new Between<>(col, Range.open(start.minusMonths(1), start))),
                index.evaluateBetween(new Between<>(col, Range.atMost(end.plusMonths(1))))))
        .allSatisfy(Range::isEmpty);
  }
}
