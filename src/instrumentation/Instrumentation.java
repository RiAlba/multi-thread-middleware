package instrumentation;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Class that handles the logging. It stores the log messages and provides a method to log all the current messages into
 * a file. Eveything needs to be thread-safe.
 * 
 * @author rialba
 *
 */
public class Instrumentation {

  /**
   * Logger for the statistics.
   */
  private Logger myLogger = Logger.getLogger("myLogger");

  /**
   * List of error messages or exceptions occurred during the experiments.
   */
  private List<String> errorList = Collections.synchronizedList(new ArrayList<String>());

  /**
   * List of keys asked for in get requests per server.
   */
  private List<Long> keysPerServer = Collections.synchronizedList(new ArrayList<Long>());

  /**
   * Average size of the queue.
   */
  private AtomicInteger queueSizeSum = new AtomicInteger(0);
  private AtomicInteger queueSizeTotal = new AtomicInteger(0);

  /**
   * Constructor. Creates the format for the beginning of the log file.
   */
  public Instrumentation(int serverNum) {
    for (int i = 0; i < serverNum; i++) {
      this.keysPerServer.add(0L);
    }

    myLogger.info("[HH:MM:SS:SSS] \tRequest type: "
        + "\tProcessing time(ms), \tQueue time(ms), \tServer time(ms), \tTotal time(ms) \t(More details)\n");
  }

  /**
   * Add a number of get keys to a specific server.
   */
  public void addKeysToServer(int server, int keys) {
    // Synchronize because we're doing both a get and set
    synchronized (this.keysPerServer) {
      this.keysPerServer.set(server, this.keysPerServer.get(server) + keys);
    }
  }

  /**
   * Add new queue size.
   */
  public void addQueueSize(int size) {
    this.queueSizeSum.addAndGet(size);
    this.queueSizeTotal.incrementAndGet();
  }

  /**
   * Add an error message into the queue.
   */
  public void addError(Exception ex) {
    // Converting the stack trace to a string
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);

    this.errorList.add(sw.toString());
  }

  /**
   * Log pending statistics to file before shutting down.
   */
  public void logPendingToFile() {
    StringBuilder stringBuilder = new StringBuilder("\n");

    // Log average queue size
    double averageQueueSize = this.queueSizeSum.doubleValue() / this.queueSizeTotal.longValue();
    stringBuilder.append(String.format("%.3f \tAverage queue size\n", averageQueueSize));

    // Log number of get keys per server
    for (int i = 0; i < this.keysPerServer.size(); i++) {
      stringBuilder.append(String.format("%d \tKeys to server %d\n", this.keysPerServer.get(i), i + 1));
    }

    // Log any error in the error list
    stringBuilder.append("\nErrors:\n");
    for (String error : errorList) {
      stringBuilder.append(String.format("%s\n", error));
    }

    myLogger.info(stringBuilder.toString());
  }
}
