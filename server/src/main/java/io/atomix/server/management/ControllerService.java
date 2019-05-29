package io.atomix.server.management;

import io.grpc.Channel;

/**
 * Controller service.
 */
public interface ControllerService {

  /**
   * Returns a channel to the controller.
   *
   * @return a channel to the controller
   */
  Channel getChannel();

}
