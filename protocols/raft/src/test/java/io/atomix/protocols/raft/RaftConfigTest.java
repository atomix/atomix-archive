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
