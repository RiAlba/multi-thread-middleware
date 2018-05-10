package thread;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import helper.QueueObject;
import helper.RoundRobin;
import helper.Server;
import instrumentation.Instrumentation;

/**
 *
 *
 * @author rialba
 *
 */
public class WorkerThread implements Runnable {

  private static final int BUFFER_SIZE = 20620;

  private int id;
  
  /**
   * Handler for logging.
   */
  private Instrumentation instrumentation;

  /**
   * Queue that stores the client requests. Passed by the middleware in initalization.
   */
  private LinkedBlockingQueue<QueueObject> queue;

  /**
   * Component which can examine one or more NIO channel's and determine which channels are ready for e.g. reading or
   * writing. Used to handle multiple channels with a single thread.
   */
  private Selector selector;

  /**
   * List of channels that are connected to the server's TCP network sockets.
   */
  private ArrayList<SocketChannel> serverSocketList;

  /**
   * Buffer used to redirect the request to the servers
   */
  private ByteBuffer buffer;

  /**
   * Handler for simple round robin load balancing.
   */
  private RoundRobin roundRobin;

  /**
   * Control if multi-GET operations are treated as regular requests or splitted into a set of smaller multi-GET
   * requests.
   */
  private boolean readSharded = false;

  /**
   * Control if the thread is running or shutting down (because the middleware is shutting down).
   */
  private boolean isRunning = true;

  /**
   * Constructor that initializes the worker thread. It opens the connection with the servers.
   *
   * @param queue
   * @param serverList
   * @param roundRobin
   * @param readSharded
   * @param instrumentation
   * @throws IOException
   */
  public WorkerThread(int id, LinkedBlockingQueue<QueueObject> queue, ArrayList<Server> serverList, RoundRobin roundRobin,
      boolean readSharded, Instrumentation instrumentation) throws IOException {

    this.id = id;
    
    this.instrumentation = instrumentation;

    this.selector = Selector.open();

    this.serverSocketList = new ArrayList<SocketChannel>();

    this.buffer = ByteBuffer.allocate(BUFFER_SIZE); // Allocate buffer of 20,620 bytes

    this.queue = queue;

    // Connect to servers
    for (Server s : serverList) {
      SocketChannel socketChannel = SocketChannel.open();
      socketChannel.connect(new InetSocketAddress(s.getIp(), Integer.parseInt(s.getPort())));
      socketChannel.configureBlocking(false);
      socketChannel.register(this.selector, SelectionKey.OP_READ);
      this.serverSocketList.add(socketChannel);
    }

    this.roundRobin = roundRobin;

    this.readSharded = readSharded;
  }

  /**
   * Run method of the thread. It is executed in loop until the middleware is stopped.
   */
  @Override
  public void run() {
    while (this.isRunning) {
      QueueObject queueObject;
      int queueSize = -1;
      
      synchronized (queue) {
        queueObject = queue.poll();
        queueSize = queue.size();
      }
      
      if (queueObject == null) continue;

      queueObject.setDequeueTime(System.nanoTime()); // Time measure log.
      
      this.instrumentation.addQueueSize(queueSize); // Queue size log

      byte[] byteRequest = queueObject.getRequest(), byteResponse = null;

      // Get type of the request
      StringBuilder stringBuilder = new StringBuilder();

      int index = 0;
      while (!Character.isWhitespace((char) byteRequest[index])) {
        stringBuilder.append((char) byteRequest[index]);
        index += 1;
      }

      // Send request
      String requestType = stringBuilder.toString();
      if (requestType.equals("get")) {
        if (readSharded) {
          try {
            byteResponse = handleMultiGet(queueObject, byteRequest);
          } catch (IOException ex) {
            this.instrumentation.addError(ex);
          }
        } else {
          try {
            byteResponse = handleGet(queueObject, byteRequest);
          } catch (IOException ex) {
            this.instrumentation.addError(ex);
          }
        }
      } else if (requestType.equals("set")) {
        try {
          byteResponse = handleSet(queueObject, byteRequest);
        } catch (IOException ex) {
          this.instrumentation.addError(ex);
        }
      } else {
        this.instrumentation.addError(new Exception("Received unsupported operation: " + requestType));
        continue;
      }

      /*** Send final response back to the client ***/
      this.buffer.clear();
      this.buffer.put(byteResponse);
      this.buffer.flip();
      // There is no guarantee of how many bytes the write() method writes to the SocketChannel.
      // Call until the buffer has no further bytes to write.
      while (this.buffer.hasRemaining()) {
        try {
          queueObject.getClient().write(this.buffer);
        } catch (IOException ex) {
          this.instrumentation.addError(ex);
        }
      }
      queueObject.setSentToClientTime(System.nanoTime()); // Time measure log.

      queueObject.log(id);
    }
  }

  /**
   * Handler method for GET requests. Send the request to the corresponding server and receive and treat the response. Construct
   * the final response that is going to be sent back to the client.
   *
   * @param queueObject
   * @param request
   * @return
   * @throws IOException
   */
  private byte[] handleGet(QueueObject queueObject, byte[] request) throws IOException {
    queueObject.setType("get");

    String[] keys = new String(request).split("\\s"); // Split request by whitespaces
    int keyCount = keys.length - 1; // Number of keys in the request

    // Select server with round robin
    int serverNum = this.roundRobin.getNextServer();
    SocketChannel responseSocket = serverSocketList.get(serverNum);

    this.instrumentation.addKeysToServer(serverNum, keyCount); // Load balancing log.

    /*** Send request to selected server ***/
    this.buffer.clear();
    this.buffer.put(request);
    this.buffer.flip();
    // There is no guarantee of how many bytes the write() method writes to the SocketChannel.
    // Call until the buffer has no further bytes to write.
    while (this.buffer.hasRemaining()) {
      responseSocket.write(this.buffer);
    }
    queueObject.setSentToServerTime(System.nanoTime()); // Time measure log.

    /*** Read response from server ***/
    selector.select(); // Blocks until at least one channel is ready for the events you registered for.

    byte[] response = this.getFullResponse(responseSocket, "END\r\n");
    queueObject.setReceivedFromServerTime(System.nanoTime()); // Time measure log.

    int missingResponses = keyCount - this.countSubstring("VALUE", new String(response));
    queueObject.setDetails(missingResponses + " miss / " + keyCount + " keys");

    return response;
  }

  /**
   * Handler method for multi-GET requests. Send the request to the corresponding server and receive and treat the response. Construct
   * the final response that is going to be sent back to the client.
   *
   * @param queueObject
   * @param request
   * @return
   * @throws IOException
   */
  private byte[] handleMultiGet(QueueObject queueObject, byte[] request) throws IOException {
    String[] keys = new String(request).split("\\s"); // Split request by whitespaces
    keys[keys.length - 1] = keys[keys.length - 1].replace("\r\n", ""); // Remove end of line characters
    int keyCount = keys.length - 1; // Number of keys in the request

    // GET
    if (keyCount == 1) {
      return handleGet(queueObject, request);
    }
    // Multi-GET
    else {
      queueObject.setType("multi-get");

      int keysPerServer = keyCount / serverSocketList.size(); // Minimum number of keys to put in each server
      int additionalKeys = keyCount % serverSocketList.size(); // Additional keys to put, one per server

      /*** Send request to corresponding servers. Try to balance the number of keys between servers. ***/
      int startPos = 1, sentRequestsCount = 0;
      int startServerNum = this.roundRobin.getNextServer();
      for (int i = startServerNum; i < startServerNum + serverSocketList.size(); i++) {
        if (startPos < keyCount + 1) { // There are keys left to send
          int serverNum = i % serverSocketList.size();

          int additional = (additionalKeys > 0) ? 1 : 0; // Additional key to send to this server

          this.instrumentation.addKeysToServer(serverNum, keysPerServer + additional); // Load balancing log.

          // Construct request message
          String newTextRequest = new String("get");
          for (int j = startPos; j < (startPos + keysPerServer + additional); j++) {
            newTextRequest = newTextRequest + " " + keys[j];
          }
          newTextRequest = newTextRequest + "\r\n";

          additionalKeys -= 1;
          startPos += keysPerServer + additional;

          this.buffer.clear();
          this.buffer.put(newTextRequest.getBytes());
          this.buffer.flip();
          // There is no guarantee of how many bytes the write() method writes to the SocketChannel.
          // Call until the buffer has no further bytes to write.
          while (this.buffer.hasRemaining()) {
            serverSocketList.get(serverNum).write(this.buffer);
          }

          sentRequestsCount += 1;
        }
      }
      queueObject.setSentToServerTime(System.nanoTime()); // Time measure log.

      /** Read and wait for a response from every server **/
      int receivedResponsesCount = 0;
      String[] textResponses = new String[serverSocketList.size()]; // Received responses.
      Arrays.fill(textResponses, "");

      while (receivedResponsesCount < sentRequestsCount) {
        selector.select(); // Blocks until at least one channel is ready for the events you registered for.
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator(); // Access the ready channels.
        while (iterator.hasNext()) {
          SelectionKey key = iterator.next();

          if (!key.isValid()) {
            System.err.println("Received a invalid key from a server.");
          }

          if (!key.isReadable()) {
            System.err.println("Received not readable key from a server.");
          }

          // The server channel is ready for reading.
          SocketChannel responseSocket = (SocketChannel) key.channel();

          byte[] byteResponse = this.getFullResponse(responseSocket, "END\r\n");

          String textResponse = new String(byteResponse).replace("END\r\n", ""); // Remove end of response.
          textResponses[serverSocketList.indexOf(responseSocket)] = textResponse;

          receivedResponsesCount += 1;

          iterator.remove();
        }
      }
      queueObject.setReceivedFromServerTime(System.nanoTime()); // Time measure log.

      String textResponse = "";
      for (String resp : textResponses) {
        textResponse = textResponse + resp; // Append each response in order
      }
      textResponse += "END\r\n"; // Append end of response

      byte[] response = textResponse.getBytes();

      int missingResponses = keyCount - this.countSubstring("VALUE", new String(response));
      queueObject.setDetails(missingResponses + " miss / " + keyCount + " keys");

      return response;
    }
  }

  /**
   * Handler method for SET requests. Send the request to every server and receive and treat the responses. Construct
   * the final response that is going to be sent back to the client.
   *
   * @param request
   * @return response
   * @throws IOException
   */
  private byte[] handleSet(QueueObject queueObject, byte[] request) throws IOException {
    queueObject.setType("set");

    /*** Send request to every server ***/
    for (SocketChannel socketChannel : serverSocketList) {
      this.buffer.clear();
      this.buffer.put(request);
      this.buffer.flip();
      // There is no guarantee of how many bytes the write() method writes to the SocketChannel.
      // Call until the buffer has no further bytes to write.
      while (this.buffer.hasRemaining()) {
        socketChannel.write(this.buffer);
      }
    }
    queueObject.setSentToServerTime(System.nanoTime()); // Time measure log.

    /** Read and wait for a response from every server **/
    int receivedResponsesCount = 0;
    byte[] response = null;

    while (receivedResponsesCount < serverSocketList.size()) {
      selector.select(); // Blocks until at least one channel is ready for the events you registered for.
      Iterator<SelectionKey> iterator = selector.selectedKeys().iterator(); // Access the ready channels.
      while (iterator.hasNext()) {
        SelectionKey key = iterator.next();

        if (!key.isValid()) {
          System.err.println("Received a invalid key from a server.");
        }

        if (!key.isReadable()) {
          System.err.println("Received not readable key from a server.");
        }

        // The server channel is ready for reading.
        SocketChannel responseSocket = (SocketChannel) key.channel();

        byte[] byteResponse = this.getFullResponse(responseSocket, "\r\n");

        // Check if response is success or failure
        String textResponse = new String(byteResponse);
        if (textResponse.equals("STORED\r\n")) {
          queueObject.setDetails("stored");
        } else {
          response = byteResponse; // Store received error message as final response
          queueObject.setDetails("not stored");
        }

        receivedResponsesCount += 1;

        iterator.remove();
      }
    }
    queueObject.setReceivedFromServerTime(System.nanoTime()); // Time measure log.

    /*** Construct response to send to the client. Send success or error message ***/
    return (response != null) ? response : new String("STORED\r\n").getBytes();
  }

  /**
   * Returns the response from a server as a byte array. It makes sure that the response has been fully received.
   *
   * @param responseSocket
   * @param responseEnd
   * @return
   * @throws IOException
   */
  private byte[] getFullResponse(SocketChannel responseSocket, String responseEnd) throws IOException {
    byte[] byteResponse = null;

    boolean receivedFullResponse = false;

    while (!receivedFullResponse) {
      this.buffer.clear();
      int bytesRead = responseSocket.read(this.buffer); // Read from the channel into a buffer.
      if (bytesRead == -1) {
        System.err.println("Error reading from server.");
      }
      this.buffer.flip();

      // Check if buffer contains the full response.
      byte[] partResponse = new byte[bytesRead];
      this.buffer.get(partResponse); // Read response from the buffer.
      receivedFullResponse = new String(partResponse).endsWith(responseEnd);

      // Append partial responde to the end of response.
      if (byteResponse == null) {
        byteResponse = partResponse;
      } else {
        byte[] combResponse = new byte[byteResponse.length + bytesRead];
        System.arraycopy(byteResponse, 0, combResponse, 0, byteResponse.length);
        System.arraycopy(partResponse, 0, combResponse, byteResponse.length, partResponse.length);

        byteResponse = combResponse;
      }
    }

    return byteResponse;
  }

  /**
   * Helper method to count the number of non-overlapping occurrences of a substring inside a string.
   *
   * @param subStr
   * @param str
   * @return
   */
  private int countSubstring(String subStr, String str) {
    // The result of split() will contain one more element than the delimiter,
    // the "-1" second argument makes it not discard trailing empty strings
    return str.split(Pattern.quote(subStr), -1).length - 1;
  }

  /**
   * Shutdown the thread and close connections to the servers.
   */
  public final void shutdown() {
    this.isRunning = false;

    try {
      this.selector.close();

      for (SocketChannel socketChannel : this.serverSocketList) {
        socketChannel.close();
      }
    } catch (IOException ex) {
      this.instrumentation.addError(ex);
    }
  }
}
