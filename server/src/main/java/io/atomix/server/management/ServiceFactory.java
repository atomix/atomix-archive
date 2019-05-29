package io.atomix.server.management;

/**
 * Service factory.
 */
public interface ServiceFactory<T> {

  /**
   * Returns a service for the given member.
   *
   * @param target the member target
   * @return the service for the given member
   */
  T getService(String target);

  /**
   * Returns a service for the given member.
   *
   * @param host the member host
   * @param port the member port
   * @return the service for the given member
   */
  T getService(String host, int port);

}
