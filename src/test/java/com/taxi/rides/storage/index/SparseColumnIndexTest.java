package com.taxi.rides.storage.index;

import static org.assertj.core.api.Assertions.assertThatObject;

import com.google.common.collect.Range;
import com.taxi.rides.storage.QueryPredicate.Between;
import com.taxi.rides.storage.schema.Column;
import com.taxi.rides.storage.schema.datatypes.LongDataType;
import org.junit.jupiter.api.Test;

public class SparseColumnIndexTest {

  @Test
  void testWithHighestGranularity() {
    var column = new Column<>("col", new LongDataType());
    var index = new SparseColumnIndex<>(column, 1);

    for (int i = 0; i < 1000; i++) {
      index.addEntry(i, i * 200L);
    }

    for (int i = 0; i < 1000; i++) {
      int rowId = i;
      assertThatObject(
              index.evaluateBetween(new Between<>(column, Range.closed(i * 200L, i * 200L))))
          .matches(r -> r.lowerEndpoint() == rowId && r.upperEndpoint() == rowId);
    }

    assertThatObject(index.evaluateBetween(new Between<>(column, Range.closed(0 * 200L, 5 * 200L))))
        .matches(r -> r.lowerEndpoint() == 0 && r.upperEndpoint() == 5);
  }

  @Test
  void testWithModestGranularity() {
    var column = new Column<>("col", new LongDataType());
    int step = 128;
    var index = new SparseColumnIndex<>(column, step);

    long maxRowId = step * 10;
    for (long i = 0; i < maxRowId; i++) {
      index.addEntry(i, i);
    }

    assertThatObject(index.evaluateBetween(new Between<>(column, Range.closed(0L, (long) step))))
        .matches(r -> r.lowerEndpoint() == 0 && r.upperEndpoint() == step);

    for (long i = 0; i < maxRowId; i += step) {
      long rowId = i;
      assertThatObject(
              index.evaluateBetween(new Between<>(column, Range.closed(rowId, rowId + (step / 2)))))
          .matches(
              r ->
                  (!r.hasUpperBound() && r.lowerEndpoint() == step * 9)
                      || (r.lowerEndpoint() == rowId && r.upperEndpoint() == rowId + step));
    }
  }

  @Test
  void testOutsideRangeWithIndexWithHighestGranularity() {
    var column = new Column<>("col", new LongDataType());
    var index = new SparseColumnIndex<>(column, 1);

    for (int i = 0; i < 1000; i++) {
      index.addEntry(i, (long) i);
    }

    assertThatObject(index.evaluateBetween(new Between<>(column, Range.closed(-100L, -1L))))
        .matches(Range::isEmpty);

    assertThatObject(index.evaluateBetween(new Between<>(column, Range.openClosed(1000L, 2000L))))
        .matches(Range::isEmpty);

    assertThatObject(index.evaluateBetween(new Between<>(column, Range.atLeast(1001L))))
        .matches(Range::isEmpty);

    assertThatObject(index.evaluateBetween(new Between<>(column, Range.atMost(-1L))))
        .matches(Range::isEmpty);

    // TODO: add support for open ended ranges to the sparse index. Returned ranges should be empty
    assertThatObject(index.evaluateBetween(new Between<>(column, Range.closedOpen(-100L, 0L))))
        .matches(r -> r.equals(Range.closed(0L, 0L)));
    assertThatObject(index.evaluateBetween(new Between<>(column, Range.closedOpen(0L, 0L))))
        .matches(r -> r.equals(Range.closed(0L, 0L)));
  }
}
