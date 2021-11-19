package com.taxi.rides.storage.index;

import com.google.common.base.Preconditions;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Row locator maintenance sparse index of row locations, e.g., it records positions of each Nth row
 * in dataset.
 */
public final class RowLocator {

  private final NavigableMap<Long, Long> rowOffsets = new TreeMap<>();
  private final int markPeriod;
  private int leftToSkip;

  public RowLocator(int markPeriod) {
    Preconditions.checkArgument(markPeriod > 0, "Mark period should be > 0");
    this.markPeriod = markPeriod;
  }

  public void addRow(long rowId, long rowOffset) {
    leftToSkip--;
    if (leftToSkip <= 0) {
      leftToSkip = markPeriod;
      rowOffsets.put(rowId, rowOffset);
    }
  }

  /**
   * Returns offset of the row with the greatest ID less than or equal to passed ID.
   */
  public long getClosestOffset(long rowId) {
    var entry = rowOffsets.floorEntry(rowId);
    return entry != null ? entry.getValue() : 0;
  }
}
