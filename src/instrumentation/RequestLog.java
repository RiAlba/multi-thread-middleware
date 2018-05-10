package instrumentation;

import java.util.logging.Logger;

/**
 * Class that stores all the statistics relative to a client request.
 * 
 * @author rialba
 *
 */
public class RequestLog {

  /**
   * Logger for the statistics.
   */
  private Logger myLogger = Logger.getLogger("myLogger");
  
  /**
   * Time (HH:MM:SS) when the request was received in the middleware.
   */
  public String time = "";

  /**
   * Type of the request.
   */
  public String type = "";

  /**
   * Details of the request. Number of cache misses for get requests. Stored or not stored for set requests.
   */
  public String details = "";

  /**
   * Time (ms) when the request was received in the middleware.
   */
  public double receiveFromClient = 0L;

  /**
   * Time (ms) when the request was sent back to the client.
   */
  public double sendToClient = 0L;

  /**
   * Time (ms) when the request was put into the queue.
   */
  public double enqueue = 0L;

  /**
   * Time (ms) when the request was taken from the queue.
   */
  public double dequeue = 0L;

  /**
   * Time (ms) when the request was sent to the server.
   */
  public double sendToServer = 0L;

  /**
   * Time (ms) when a response for the request was received from the server.
   */
  public double receiveFromServer = 0L;

  /**
   * Empty constructor.
   */
  public RequestLog() {
  }

  /**
   * Log a string containing the information about the request.
   */
  public void log(int threadId) {
    double queueTime = this.dequeue - this.enqueue;
    double serverTime = this.receiveFromServer - this.sendToServer;
    double totalTime = this.sendToClient - this.receiveFromClient;
    double processingTime = totalTime - queueTime - serverTime;

    myLogger.info(String.format("[%s] \t%s: \t\t%f ms, \t\t%f ms, \t\t%f ms, \t\t%f ms \t(%s) \t(%d)\n", this.time, this.type,
        processingTime, queueTime, serverTime, totalTime, this.details, threadId));
  }
}
