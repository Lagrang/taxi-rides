package com.taxi.rides.storage;

import com.google.common.collect.Range;
import com.taxi.rides.storage.index.ColumnIndex;
import com.taxi.rides.storage.index.ColumnIndexes;
import com.taxi.rides.storage.index.RowLocator;
import com.taxi.rides.storage.schema.Column;
import com.taxi.rides.storage.schema.Schema;
import de.siegmar.fastcsv.reader.CloseableIterator;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import de.siegmar.fastcsv.reader.NamedCsvReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class CsvStorageFile implements StorageFile {

  private final Path csvPath;
  private final Schema csvSchema;
  private final RowLocator rowLocator;
  private final ColumnIndexes indexes;

  public CsvStorageFile(
      Path csvPath,
      Schema expectedSchema,
      RowLocator rowLocator,
      List<ColumnIndex> indexesToPopulate) {
    this.csvPath = Objects.requireNonNull(csvPath, "CSV file path missed");

    try (var reader = NamedCsvReader.builder().fieldSeparator(',').build(csvPath)) {
      // redefine schema object according to order of columns inside CSV file
      var columns = new ArrayList<Column>();
      for (String colName : reader.getHeader()) {
        // no schema evolution right now: fail if we can't find some required column in CSV
        Column column =
            expectedSchema
                .getColumn(colName)
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            colName + " column not exists in CSV file " + csvPath));
        columns.add(column);
      }
      this.csvSchema = new Schema(columns);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try (var reader = CsvReader.builder().fieldSeparator(',').build(csvPath);
        var iterator = reader.iterator()) {
      populateIndexes(iterator, csvSchema, rowLocator, indexesToPopulate);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.rowLocator = rowLocator;
    this.indexes = new ColumnIndexes(indexesToPopulate);
  }

  private static void populateIndexes(
      Iterator<CsvRow> reader,
      Schema schema,
      RowLocator rowLocator,
      List<ColumnIndex> indexesToPopulate) {

    if (indexesToPopulate.isEmpty()) {
      return;
    }

    // compute for each index, where required column located inside CSV row(e.g., column index)
    var indexes =
        indexesToPopulate.stream()
            .map(
                index ->
                    new IndexState(index, schema.getColumnIndex(index.column().name()).getAsInt()))
            .collect(Collectors.toList());

    reader.next(); // skip CSV header
    reader.forEachRemaining(
        row -> {
          // row ID will start from 0: ordinal number starts from 1, and it accounts header line
          long rowId = row.getOriginalLineNumber() - 2;
          rowLocator.addEntry(rowId, row.getStartingOffset());
          for (IndexState indexState : indexes) {
            var rawColVal = row.getField(indexState.columnIndex);
            var colValue = indexState.index.column().dataType().parseFrom(rawColVal);
            indexState.index.addEntry(rowId, colValue);
          }
        });
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

    Range<Long> rowsRange = indexes.evaluate(predicate);
    if (rowsRange.isEmpty()) {
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
    private final CloseableIterator<CsvRow> rowIter;
    private final int[] colIdx;
    private final SeekableByteChannel fileChannel;
    private final long startRowOffset;
    private final long endRowOffset;
    private long bytesRead;

    public CsvIter(int[] colIdx, Range<Long> offsets) throws IOException {
      this.colIdx = colIdx;
      fileChannel = Files.newByteChannel(csvPath, StandardOpenOption.READ);
      if (offsets.hasLowerBound()) {
        fileChannel.position(offsets.lowerEndpoint());
        startRowOffset = offsets.lowerEndpoint();
      } else {
        startRowOffset = 0;
      }
      if (offsets.hasUpperBound()) {
        endRowOffset = offsets.upperEndpoint();
      } else {
        endRowOffset = fileChannel.size();
      }
      csvReader =
          CsvReader.builder().build(Channels.newReader(fileChannel, StandardCharsets.UTF_8));
      rowIter = csvReader.iterator();
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
      return endRowOffset > startRowOffset + bytesRead && rowIter.hasNext();
    }

    @Override
    public Row next() {
      var result = new Row(colIdx.length);
      var row = rowIter.next();
      bytesRead = row.getStartingOffset();
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
  }
}
