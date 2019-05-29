package io.atomix.server.protocol;

import com.google.protobuf.Message;
import io.atomix.server.management.ProtocolManagementService;
import io.atomix.utils.ConfiguredType;
import io.atomix.utils.component.Managed;

/**
 * Atomix server protocol.
 */
public interface Protocol extends Managed {

  /**
   * Service type.
   */
  interface Type<C extends Message> extends ConfiguredType<C> {

    /**
     * Creates a new protocol instance from the given configuration.
     *
     * @param config            the protocol configuration
     * @param managementService the protocol management service
     * @return the protocol instance
     */
    Protocol newProtocol(C config, ProtocolManagementService managementService);
  }

}
