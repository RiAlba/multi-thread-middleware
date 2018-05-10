package helper;

/**
 * Class to represent a server.
 * 
 * @author rialba
 *
 */
public class Server {

  private String ip;
  private String port;

  /**
   * Constructor.
   * 
   * @param ip
   * @param port
   */
  public Server(String ip, String port) {
    this.ip = ip;
    this.port = port;
  }

  /**
   * Get the server ip.
   * 
   * @return
   */
  public String getIp() {
    return this.ip;
  }

  /**
   * Get the server port.
   * 
   * @return
   */
  public String getPort() {
    return this.port;
  }

}
