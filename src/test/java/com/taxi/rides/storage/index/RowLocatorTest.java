package com.taxi.rides.storage.index;

import static org.assertj.core.api.Assertions.assertThatObject;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

public class RowLocatorTest {

  @Test
  void testRowIdSearch() {
    var rowLocator = new RowLocator(1);
    var maxRowId = 1000;
    for (long i = 0; i < maxRowId; i++) {
      // use negative values to check that sparse index will not fail if row ID will have
      // unexpected format.
      rowLocator.addEntry(i, i * -1);
    }

    for (long i = 0; i < maxRowId; i += 100) {
      long rowId = i;
      if (i + 100 >= maxRowId) {
        assertThatObject(rowLocator.getClosestOffsets(Range.closedOpen(i, i + 100)))
            .matches(r -> r.equals(Range.atLeast(rowId * -1)));
      } else {
        // TODO: add support for open ended ranges to the sparse index. Returned range should be
        // '[..)', not '[..]'
        var expected =
            Range.closed(
                Math.min(rowId * -1, rowId * -1 - 100), Math.max(rowId * -1, rowId * -1 - 100));
        assertThatObject(rowLocator.getClosestOffsets(Range.closedOpen(i, i + 100)))
            .matches(r -> r.equals(expected));
      }
    }
  }
}
