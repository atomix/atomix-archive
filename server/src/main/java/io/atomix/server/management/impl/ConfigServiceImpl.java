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
package io.atomix.server.management.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;

import com.google.protobuf.util.JsonFormat;
import io.atomix.api.controller.NodeConfig;
import io.atomix.api.controller.PartitionConfig;
import io.atomix.api.controller.PartitionId;
import io.atomix.server.management.ConfigService;
import io.atomix.utils.component.Component;

/**
 * Configuration service.
 */
@Component
public class ConfigServiceImpl implements ConfigService {

  static PartitionConfig config = null;

  /**
   * Loads the given configuration file.
   *
   * @param file the configuration file
   */
  public static void load(File file) throws IOException {
    try (FileInputStream is = new FileInputStream(file)) {
      PartitionConfig.Builder builder = PartitionConfig.newBuilder();
      JsonFormat.parser().ignoringUnknownFields().merge(new InputStreamReader(is), builder);
      config = builder.build();
    }
  }

  @Override
  public PartitionId getPartition() {
    return config.getPartition();
  }

  @Override
  public NodeConfig getController() {
    return config.getController();
  }

  @Override
  public NodeConfig getNode() {
    return config.getNode();
  }

  @Override
  public Collection<NodeConfig> getCluster() {
    return config.getMembersList();
  }
}
