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
package io.atomix.core.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Maps;
import io.atomix.core.AtomixConfig;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.ConfigService;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;

/**
 * Default configuration service.
 */
@Component(AtomixConfig.class)
public class ConfigManager implements ConfigService, Managed<AtomixConfig> {
  private final Map<String, PrimitiveConfig> defaultConfigs = Maps.newConcurrentMap();
  private final Map<String, PrimitiveConfig> configs = Maps.newConcurrentMap();

  @Override
  @SuppressWarnings("unchecked")
  public <C extends PrimitiveConfig<C>> C getConfig(String primitiveName, PrimitiveType primitiveType) {
    C config = (C) configs.get(primitiveName);
    if (config != null) {
      return config;
    }
    if (primitiveType == null) {
      return null;
    }
    config = (C) defaultConfigs.get(primitiveType.name());
    if (config != null) {
      return config;
    }
    return (C) primitiveType.newConfig();
  }

  @Override
  public CompletableFuture<Void> start(AtomixConfig config) {
    defaultConfigs.putAll(config.getPrimitiveDefaults());
    configs.putAll(config.getPrimitives());
    return CompletableFuture.completedFuture(null);
  }
}
