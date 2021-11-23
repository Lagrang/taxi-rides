package com.taxi.rides.storage;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.taxi.rides.storage.index.ColumnIndex;
import com.taxi.rides.storage.index.ColumnIndexes;
import com.taxi.rides.storage.index.RowOffsetLocator;
import com.taxi.rides.storage.schema.Column;
import com.taxi.rides.storage.schema.Schema;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class CsvStorageFile implements StorageFile {

  private final Path csvPath;
  private final Schema csvSchema;
  private final RowOffsetLocator rowLocator;
  private final ColumnIndexes indexes;
  private long rowsCount;
  private final long fileStartOffset;
  private long fileEndOffset;
  private long lastRowOffset;

  public CsvStorageFile(
      Path csvPath,
      Schema expectedSchema,
      RowOffsetLocator rowLocator,
      List<ColumnIndex> indexesToPopulate,
      long startAt,
      long splitSize) {
    this.csvPath = Objects.requireNonNull(csvPath, "CSV file path missed");
    this.csvSchema = expectedSchema;
    this.rowLocator = rowLocator;
    this.indexes = new ColumnIndexes(indexesToPopulate);
    this.fileStartOffset = startAt;

    try (var fileChannel = Files.newByteChannel(csvPath, StandardOpenOption.READ)) {
      fileChannel.position(fileStartOffset);
      try (var reader =
              CsvReader.builder().build(Channels.newReader(fileChannel, StandardCharsets.UTF_8));
          var iterator = reader.iterator()) {
        populateIndexes(splitSize, iterator, indexesToPopulate);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void populateIndexes(
      long splitSize, Iterator<CsvRow> reader, List<ColumnIndex> indexesToPopulate)
      throws IOException {

    // compute for each index, where required column located inside CSV row(e.g., column index)
    var indexes =
        indexesToPopulate.stream()
            .map(
                index ->
                    new IndexState(
                        index, csvSchema.getColumnIndex(index.column().name()).getAsInt()))
            .collect(Collectors.toList());

    // skip CSV header if we start from file beginning
    if (fileStartOffset == 0) {
      reader.next();
    }

    long splitPoint = fileStartOffset + splitSize;
    fileEndOffset = fileStartOffset;
    lastRowOffset = fileStartOffset;
    while (reader.hasNext()) {
      CsvRow row = reader.next();
      rowsCount++;
      // row ID will start from 0: ordinal number starts from 1,
      // and also account header line if needed.
      long rowId = row.getOriginalLineNumber() - (fileStartOffset == 0 ? 2 : 1);
      lastRowOffset = fileStartOffset + row.getStartingOffset();
      rowLocator.addEntry(rowId, lastRowOffset);
      for (IndexState indexState : indexes) {
        var rawColVal = row.getField(indexState.columnIndex);
        var colValue = indexState.index.column().dataType().parseFrom(rawColVal);
        indexState.index.addEntry(rowId, colValue);
      }

      if (lastRowOffset >= splitPoint) {
        // reach split point
        break;
      }
    }

    if (reader.hasNext()) {
      // reach split point
      fileEndOffset = fileStartOffset + reader.next().getStartingOffset() - 1;
    } else {
      // reach end of file
      fileEndOffset = Files.size(csvPath) - 1;
    }
  }

  public long endOffset() {
    return fileEndOffset;
  }

  @Override
  public RowReader openReader(List<Column> requiredColumns, QueryPredicate predicate)
      throws IOException {
    int[] colIdx = new int[requiredColumns.size()];
    int i = 0;
    for (Column reqCol : requiredColumns) {
      int index =
          csvSchema
              .getColumnIndex(reqCol.name())
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          reqCol + " not contained in CSV file" + csvPath));
      colIdx[i++] = index;
    }

    if (rowsCount == 0) {
      System.out.println(csvPath.getFileName() + ": skipped because empty.");
      return RowReader.empty(new Schema(requiredColumns));
    }
    Range<Long> rowsRange = indexes.evaluatePredicate(predicate);
    if (rowsRange.isEmpty()) {
      System.out.println(
          csvPath.getFileName()
              + "("
              + fileStartOffset
              + ":"
              + fileEndOffset
              + ")"
              + ": skipped using indexes.");
      return RowReader.empty(new Schema(requiredColumns));
    }
    // compute byte offsets of rows which should be scanned in this file according to index data
    Range<Long> rowOffsets = rowLocator.getClosestOffsets(rowsRange);
    return new CsvIter(colIdx, rowOffsets);
  }

  record IndexState(ColumnIndex index, int columnIndex) {}

  private class CsvIter implements RowReader {

    private final Schema readerSchema;
    private final CsvReader csvReader;
    private final PeekingIterator<CsvRow> rowIter;
    private final int[] colIdx;
    private final SeekableByteChannel fileChannel;
    private final long startRowOffset;
    private final long endRowOffset;
    private long rowsRead;

    public CsvIter(int[] colIdx, Range<Long> offsets) throws IOException {
      this.colIdx = colIdx;
      fileChannel = Files.newByteChannel(csvPath, StandardOpenOption.READ);
      if (offsets.hasLowerBound()) {
        fileChannel.position(offsets.lowerEndpoint());
        startRowOffset = offsets.lowerEndpoint();
      } else {
        fileChannel.position(fileStartOffset);
        startRowOffset = fileStartOffset;
      }
      if (offsets.hasUpperBound()) {
        endRowOffset = offsets.upperEndpoint();
      } else {
        endRowOffset = lastRowOffset;
      }
      csvReader =
          CsvReader.builder().build(Channels.newReader(fileChannel, StandardCharsets.UTF_8));
      rowIter = Iterators.peekingIterator(csvReader.iterator());
      if (startRowOffset == 0) {
        // we start from beginning of CSV file and should skip header
        rowIter.next();
      }
      readerSchema =
          new Schema(
              Arrays.stream(colIdx).mapToObj(csvSchema::getColumnAt).collect(Collectors.toList()));
    }

    @Override
    public void close() throws Exception {
      fileChannel.close();
      csvReader.close();
    }

    @Override
    public boolean hasNext() {
      return rowIter.hasNext()
          && endRowOffset >= startRowOffset + rowIter.peek().getStartingOffset();
    }

    @Override
    public Row next() {
      rowsRead++;
      var result = new Row(colIdx.length);
      var row = rowIter.next();
      int outputSchemaIdx = 0;
      for (int idx : colIdx) {
        String rawVal = row.getField(idx);
        var columnValue = csvSchema.getColumnAt(idx).dataType().parseFrom(rawVal);
        result.set(outputSchemaIdx++, columnValue);
      }
      return result;
    }

    @Override
    public Schema schema() {
      return readerSchema;
    }

    @Override
    public void printStats() {
      assert (rowsRead <= rowsCount);
      System.out.println(
          csvPath.getFileName()
              + "("
              + fileStartOffset
              + ":"
              + fileEndOffset
              + ")"
              + ": "
              + rowsRead
              + " rows "
              + "read/total "
              + "rows="
              + rowsCount
              + ".");
    }
  }
}
