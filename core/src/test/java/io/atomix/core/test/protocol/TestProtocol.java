/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.test.protocol;

import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test primitive protocol.
 */
public class TestProtocol implements ProxyProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns a new test protocol builder.
   *
   * @return a new test protocol builder
   */
  public static TestProtocolBuilder builder() {
    return new TestProtocolBuilder(new TestProtocolConfig());
  }

  /**
   * Multi-Raft protocol type.
   */
  public static final class Type implements PrimitiveProtocol.Type<TestProtocolConfig> {
    private static final String NAME = "multi-raft";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public TestProtocolConfig newConfig() {
      return new TestProtocolConfig();
    }

    @Override
    public PrimitiveProtocol newProtocol(TestProtocolConfig config) {
      return new TestProtocol(config);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TestProtocol.class);

  private final TestProtocolConfig config;

  TestProtocol(TestProtocolConfig config) {
    this.config = config;
  }

  @Override
  public Type type() {
    return TYPE;
  }

  @Override
  public String group() {
    return config.getGroup();
  }
}
