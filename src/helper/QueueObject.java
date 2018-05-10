package helper;

import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import instrumentation.RequestLog;

/**
 * Object that is stored in the internal request queue. Contains all the information that a worker thread needs to send
 * the request and receive the response. Represents a request sent by the client.
 * 
 * @author rialba
 *
 */
public class QueueObject {
  /**
   * Request in byte format.
   */
  private byte[] request;

  /**
   * Client that sent the request.
   */
  private SocketChannel client;

  /**
   * Object that stores information for this request.
   */
  private RequestLog requestLog;

  /**
   * Constructor.
   * 
   * @param request
   * @param client
   */
  public QueueObject(SocketChannel client) {
    this.requestLog = new RequestLog();
    this.client = client;
    // The request will be initialized by using its corresponding setter method.
  }

  /**
   * Return the content of the request as an array of bytes.
   * 
   * @return
   */
  public byte[] getRequest() {
    return this.request;
  }

  /**
   * Return the SocketChannel of the client who sent the request.
   * 
   * @return
   */
  public SocketChannel getClient() {
    return this.client;
  }

  /**
   * Return the RequestLog containing the request statistics.
   * 
   * @return
   */
  public RequestLog getRequestLog() {
    return this.requestLog;
  }

  /**
   * Add the request as a byte array.
   * 
   * @param request
   */
  public void setRequest(byte[] request) {
    this.request = request;
  }

  /**
   * Set now as the time the request was received from the client (human-readable). For logging.
   */
  public void setTime() {
    String currentTime = new SimpleDateFormat("HH:mm:ss:SSS", Locale.ENGLISH).format(new Date());
    this.requestLog.time = currentTime;
  }

  /**
   * Set the type of the request. For logging.
   * 
   * @param type
   */
  public void setType(String type) {
    this.requestLog.type = type;
  }

  /**
   * Set additional information about the request. Cache misses for gets and status for ets. For logging.
   * 
   * @param details
   */
  public void setDetails(String details) {
    this.requestLog.details = details;
  }

  /**
   * Set the time the request was received from the client. For logging.
   * 
   * @param time
   */
  public void setReceivedFromClientTime(Long time) {
    this.requestLog.receiveFromClient = time / 1000000.0;
  }

  /**
   * Set the time the request was put into the queue. For logging.
   * 
   * @param time
   */
  public void setEnqueueTime(Long time) {
    this.requestLog.enqueue = time / 1000000.0;
  }

  /**
   * Set the time the request was taken from the queue. For logging.
   * 
   * @param time
   */
  public void setDequeueTime(Long time) {
    this.requestLog.dequeue = time / 1000000.0;
  }

  /**
   * Set the time the request was sent to the server. For logging.
   * 
   * @param time
   */
  public void setSentToServerTime(Long time) {
    this.requestLog.sendToServer = time / 1000000.0;
  }

  /**
   * Set the time the request was received from the server. For logging.
   * 
   * @param time
   */
  public void setReceivedFromServerTime(Long time) {
    this.requestLog.receiveFromServer = time / 1000000.0;
  }

  /**
   * Set the time the request was sent back to the client. For logging.
   * 
   * @param time
   */
  public void setSentToClientTime(Long time) {
    this.requestLog.sendToClient = time / 1000000.0;
  }
  
  /**
   * Log the request log to a file.
   */
  public void log(int threadId) {
    this.requestLog.log(threadId);
  }
}
