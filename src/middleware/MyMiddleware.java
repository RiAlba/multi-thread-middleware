package middleware;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import helper.QueueObject;
import helper.RoundRobin;
import helper.Server;
import instrumentation.Instrumentation;
import thread.WorkerThread;

/**
 * Class to represent the middleware.
 * 
 * Connect and receive request from the clients.
 * 
 * @author rialba
 *
 */
public class MyMiddleware {

  private static final int BUFFER_SIZE = 10015;

  /**
   * Handler for logging.
   */
  private Instrumentation instrumentation;

  /**
   * Channel that can listen for incoming TCP connections.
   */
  private ServerSocketChannel clientSocket;

  /**
   * Component which can examine one or more NIO channel's and determine which channels are ready for e.g. reading or
   * writing. Used to handle multiple channels with a single thread.
   */
  private Selector selector;

  /**
   * List of servers for the servers.
   */
  private ArrayList<Server> serverList;

  /**
   * Queue that stores the client requests. Use .add() to insert into the queue and .poll() to remove from the queue.
   */
  private LinkedBlockingQueue<QueueObject> queue;

  /**
   * List of worker threads.
   */
  private ArrayList<WorkerThread> workerThreadList;

  /**
   * Buffer used to store a client request.
   */
  private ByteBuffer buffer;

  /**
   * Handler for simple round robin load balancing.
   */
  private RoundRobin roundRobin;

  /**
   * Control if the middleware is running or shutting down.
   */
  private boolean isRunning = true;

  /**
   * Creates a new middleware and binds it.
   * 
   * @param myIp
   * @param myPort
   * @param mcAddresses
   * @param numThreadsPTP
   * @param readSharded
   * @throws IOException
   */
  public MyMiddleware(String myIp, int myPort, List<String> mcAddresses, int numThreadsPTP, boolean readSharded)
      throws IOException {
    this.instrumentation = new Instrumentation(mcAddresses.size());

    this.buffer = ByteBuffer.allocate(BUFFER_SIZE); // Allocate buffer of 10,015 bytes.

    this.clientSocket = ServerSocketChannel.open();

    this.selector = Selector.open();

    this.queue = new LinkedBlockingQueue<QueueObject>();

    this.clientSocket.socket().bind(new InetSocketAddress(myIp, myPort));
    this.clientSocket.configureBlocking(false); // Non-blocking mode to be used with a selector.
    this.clientSocket.register(this.selector, SelectionKey.OP_ACCEPT); // Registering channel (to accept incoming
                                                                       // connections) with the selector.

    this.serverList = new ArrayList<Server>();
    for (String mcAddress : mcAddresses) {
      String[] address = mcAddress.split(":");

      this.serverList.add(new Server(address[0], address[1])); // IP and port.
    }
    this.roundRobin = new RoundRobin(this.serverList.size());

    this.workerThreadList = new ArrayList<WorkerThread>();
    for (int i = 0; i < numThreadsPTP; i++) {
      WorkerThread workerThread = new WorkerThread(i+1, this.queue, this.serverList, this.roundRobin, readSharded,
          this.instrumentation);

      workerThreadList.add(workerThread);

      new Thread(workerThread).start(); // Run thread
    }
  }

  /**
   * Core run method. This is not a thread safe method, however it is non-blocking. If an exception is encountered it
   * will be thrown wrapped in a RuntimeException, and the server will automatically be shutDown.
   */
  public void run() {
    while (this.isRunning) {
      try {
        if (selector.isOpen()) {
          this.selector.select(); // Select keys whose corresponding channels are ready for I/O operations.
        }

        Iterator<SelectionKey> keyIterator = Collections.emptyIterator();

        if (selector.isOpen()) {
          keyIterator = selector.selectedKeys().iterator();
        }

        while (keyIterator.hasNext()) {
          SelectionKey key = keyIterator.next();
          keyIterator.remove();

          if (!key.isValid()) {
            continue;
          }

          try {
            if (key.isAcceptable()) { // The channel has an incoming connection.
              SocketChannel client = clientSocket.accept();
              client.configureBlocking(false); // Non-blocking mode
              client.register(selector, SelectionKey.OP_READ); // Registering channel (to read data) with
                                                               // the selector.

            } else if (key.isReadable()) { // The channel has data ready to be read
              SocketChannel clientSocket = (SocketChannel) key.channel(); // Access the client.

              QueueObject queueObject = new QueueObject(clientSocket);

              byte[] request = this.getFullRequest(clientSocket);
              if (request == null) {
              	continue;
              }

              queueObject.setTime(); // Time measure log.
              queueObject.setReceivedFromClientTime(System.nanoTime()); // Time measure log.
              queueObject.setRequest(request);
              
              synchronized (queue) {
                queueObject.setEnqueueTime(System.nanoTime()); // Time measure log.
                queue.put(queueObject);
                this.instrumentation.addQueueSize(queue.size()); // Queue size log.
              }
            }
          } catch (Exception ex) {
            this.instrumentation.addError(ex);
          }
        }

      } catch (IOException ex) {
        this.instrumentation.addError(ex);
        this.shutdown();
      }
    }
  }

  /**
   * Returns a client request as a byte array. It makes sure that the request has been fully received.
   * 
   * @param responseSocket
   * @param responseEnd
   * @return
   * @throws IOException
   */
  private byte[] getFullRequest(SocketChannel responseSocket) throws IOException {
    byte[] byteRequest = null;

    boolean receivedFullResponse = false;

    while (!receivedFullResponse) {
      this.buffer.clear();
      int bytesRead = responseSocket.read(this.buffer); // Read from the channel into a buffer.
      if (bytesRead == -1) {
        responseSocket.close();
        break;
      } else if (bytesRead > 0) {
        this.buffer.flip();

        // Check if buffer contains the full response.
        byte[] partResponse = new byte[bytesRead];
        this.buffer.get(partResponse); // Read response from the buffer.
        receivedFullResponse = new String(partResponse).endsWith("\r\n");

        // Append partial responde to the end of response.
        if (byteRequest == null) {
          byteRequest = partResponse;
        } else {
          byte[] combResponse = new byte[byteRequest.length + bytesRead];
          System.arraycopy(byteRequest, 0, combResponse, 0, byteRequest.length);
          System.arraycopy(partResponse, 0, combResponse, byteRequest.length, partResponse.length);

          byteRequest = combResponse;
        }
      }
    }

    return byteRequest;
  }

  /**
   * Shutdown the middleware, preventing it from handling any more requests.
   */
  public final void shutdown() {
    this.isRunning = false;

    // Shut down worker threads.
    for (WorkerThread workerThread : workerThreadList) {
      workerThread.shutdown();
    }

    // Shutdown connection with the clients.
    try {
      this.selector.close();
      this.clientSocket.close();
    } catch (IOException ex) {
      this.instrumentation.addError(ex);
    }

    this.instrumentation.logPendingToFile();
  }

}
