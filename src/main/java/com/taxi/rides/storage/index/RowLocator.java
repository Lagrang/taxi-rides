package com.taxi.rides.storage.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Class maintenances sparse index of row locations inside file. It records file offset for each Nth
 * row.
 *
 * <p>This class contains methods which used to find out file offset range which corresponds to some
 * row range. Usually, these methods called with rows range computed by evaluating predicates on
 * indexes.
 */
public final class RowLocator {

  private final NavigableMap<Long, Long> rowOffsets = new TreeMap<>();
  private final int markPeriod;
  private int leftToSkip;

  public RowLocator(int markPeriod) {
    Preconditions.checkArgument(markPeriod > 0, "Mark period should be > 0");
    this.markPeriod = markPeriod;
  }

  /** Add row location to index. */
  public void addRow(long rowId, long rowOffset) {
    leftToSkip--;
    if (leftToSkip <= 0) {
      leftToSkip = markPeriod;
      rowOffsets.put(rowId, rowOffset);
    }
  }

  /**
   * Returns file offsets which covers passed row range. Returned range is approximate: it can cover
   * more rows than requested(but never less).
   */
  public Range<Long> getClosestOffsets(Range<Long> rowRange) {
    long lower = 0;
    if (rowRange.hasLowerBound()) {
      var entry = rowOffsets.floorEntry(rowRange.lowerEndpoint());
      lower = entry != null ? entry.getValue() : 0;
    }

    long upper;
    if (rowRange.hasUpperBound()) {
      var entry = rowOffsets.ceilingEntry(rowRange.upperEndpoint());
      upper = entry != null ? entry.getValue() : 0;
      return Range.closed(lower, upper);
    } else {
      return Range.atLeast(lower);
    }
  }
}
