package com.taxi.rides;

import com.google.common.base.Stopwatch;
import com.taxi.rides.RidesTable.Settings;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;

@CommandLine.Command(name = "taxirides")
public class App implements Runnable, CommandLine.ITypeConverter<LocalDateTime> {
  private static final DateTimeFormatter DATE_FORMATTER =
      new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter();

  @CommandLine.Option(
      names = {"--data-dir", "-f"},
      required = true,
      description = "Folder with CSV files")
  private Path csvFolder;

  @CommandLine.Option(
      names = {"--from", "-b"},
      required = true,
      description = "Start date to query CSV storage")
  private LocalDateTime startDate;

  @CommandLine.Option(
      names = {"--until", "-u"},
      required = true,
      description = "End date to query CSV storage")
  private LocalDateTime endDate;

  @CommandLine.Option(
      names = {"--init-threads", "-i"},
      defaultValue = "4",
      description = "Thread pool size used to initialize CSV storage")
  private int initThreads;

  @CommandLine.Option(
      names = {"--query-threads", "-q"},
      defaultValue = "4",
      description = "Thread pool size used to query CSV storage")
  private int queryThreads;

  @CommandLine.Option(
      names = {"--index-step", "-s"},
      defaultValue = "8192",
      description = "Step used by sparse indexes(each 'step' row will be indexed)")
  private int skipIndex;

  @CommandLine.Option(
      names = {"--split-size"},
      defaultValue = "100",
      description = "Logically split CSV file when if reaches size(in MB)")
  private long splitSize;

  @CommandLine.Option(
      names = {"--no-min-max-index"},
      description = "Disable min-max index")
  boolean disableMinMaxIndex = false;

  @CommandLine.Option(
      names = {"--no-bucket-index"},
      description = "Disable bucket index")
  boolean disableBucketIndex = false;

  @CommandLine.Option(
      names = {"--no-not-null-index"},
      description = "Disable not-null index")
  boolean disableNotNullIndex = false;

  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Print help")
  boolean help;

  private RidesTable table;

  public static void main(String[] args) {
    var app = new App();
    System.exit(new CommandLine(app).registerConverter(LocalDateTime.class, app).execute(args));
  }

  @Override
  public void run() {
    if (help) {
      CommandLine.usage(new App(), System.out);
      return;
    }

    table =
        new RidesTable(
            new Settings(
                initThreads,
                queryThreads,
                skipIndex,
                splitSize * 1024 * 1024,
                disableBucketIndex,
                disableNotNullIndex,
                disableMinMaxIndex));
    var sw = Stopwatch.createStarted();
    System.out.println("Initializing from folder: " + csvFolder);
    table.init(csvFolder);
    System.out.println("Initialization took " + sw.elapsed(TimeUnit.SECONDS) + "sec");
    System.out.println();

    sw.reset();
    sw.start();
    var res = table.getAverageDistances(startDate, endDate);
    long timeInSec = sw.elapsed(TimeUnit.SECONDS);
    if (timeInSec == 0) {
      System.out.println("Query took: " + sw.elapsed(TimeUnit.MILLISECONDS) + "ms");
    } else {
      System.out.println("Query took: " + timeInSec + "sec");
    }
    System.out.println();
    System.out.println("Average distances(passengers count to average distance):");
    res.forEach((k, v) -> System.out.println(k + " : " + v));
  }

  @Override
  public LocalDateTime convert(String value) {
    return LocalDateTime.parse(value, DATE_FORMATTER);
  }
}
