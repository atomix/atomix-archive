/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.node.protocol;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.Message;
import io.atomix.node.management.ProtocolManagementService;
import io.atomix.utils.NamedType;
import io.atomix.utils.component.Managed;

/**
 * Atomix server protocol.
 */
public interface Protocol extends Managed {

  /**
   * Service type.
   */
  interface Type<C extends Message> extends NamedType {

    /**
     * Parses the configuration from the given bytes.
     *
     * @param is the configuration input stream
     * @return the configuration
     * @throws IOException
     */
    C parseConfig(InputStream is) throws IOException;

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
