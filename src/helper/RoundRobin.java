package helper;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class for simple round robin load balancing.
 * 
 * @author rialba
 *
 */
public class RoundRobin {

  /**
   * Total number of connected servers to the middleware.
   */
  private int serverNumber;

  /**
   * Integer that may be updated atomically.
   */
  private AtomicInteger serverIndex;

  /**
   * Constructor.
   * 
   * @param serverNumber
   */
  public RoundRobin(int serverNumber) {
    this.serverIndex = new AtomicInteger(0);
    this.serverNumber = serverNumber;
  }

  /**
   * Get next server.
   * 
   * @return next server
   */
  public int getNextServer() {
    return serverIndex.getAndAccumulate(serverNumber,
        (currentIndex, n) -> currentIndex == n - 1 ? 0 : currentIndex + 1);
  }

}
