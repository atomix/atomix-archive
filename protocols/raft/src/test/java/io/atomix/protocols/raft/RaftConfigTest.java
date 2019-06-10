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
package io.atomix.protocols.raft;

import io.atomix.server.ProtocolConfig;
import io.atomix.server.ServerConfig;
import io.atomix.utils.ConfiguredType;
import io.atomix.utils.config.ConfigMapper;
import io.atomix.utils.config.PolymorphicTypeMapper;
import io.atomix.utils.config.TypeRegistry;
import org.junit.Test;

/**
 * Server configuration test.
 */
public class RaftConfigTest {
  @Test
  public void testParseYamlConfig() throws Exception {
    String yaml = "partitionId: 1\n" +
        "partitionGroup:\n" +
        "  name: raft\n" +
        "  namespace: default\n" +
        "controller:\n" +
        "  id: controller\n" +
        "  host: localhost\n" +
        "  port: 5679\n" +
        "node:\n" +
        "  id: raft-1\n" +
        "  host: raft-1.default\n" +
        "  port: 5678\n" +
        "protocol:\n" +
        "  type: raft\n" +
        "  members:\n" +
        "    - id: raft-1\n" +
        "      host: raft-1.default\n" +
        "      port: 5678\n" +
        "    - id: raft-2\n" +
        "      host: raft-2.default\n" +
        "      port: 5678\n" +
        "    - id: raft-3\n" +
        "      host: raft-3.default\n" +
        "      port: 5678\n" +
        "  storage:\n" +
        "    directory: /var/lib/atomix";
    ConfigMapper mapper = new ConfigMapper(
        Thread.currentThread().getContextClassLoader(),
        new PolymorphicTypeMapper(
            ProtocolConfig.getDescriptor(),
            ProtocolConfig.getDescriptor().findFieldByName("type"),
            ProtocolConfig.getDescriptor().findFieldByName("config"),
            new TypeRegistry() {
              @Override
              public ConfiguredType getType(String name) {
                return RaftProtocol.TYPE;
              }
            }));
    ServerConfig config = mapper.loadString(ServerConfig.getDescriptor(), yaml);
    System.out.println(config);
  }
}
