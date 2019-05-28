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
package io.atomix.utils.config;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Primitives;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for applying file configurations to Atomix configuration objects.
 */
public class ConfigMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigMapper.class);
  private final ClassLoader classLoader;

  public ConfigMapper(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  /**
   * Loads the given configuration file using the mapper, falling back to the given resources.
   *
   * @param descriptor the type descriptor
   * @param files      the files to load
   * @param resources  the resources to which to fall back
   * @param <T>        the resulting type
   * @return the loaded configuration
   */
  public <T> T loadFiles(Descriptors.Descriptor descriptor, List<File> files, List<String> resources) {
    if (files == null) {
      return loadResources(descriptor, resources);
    }

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    ObjectNode config = parseFiles(mapper, files, resources);
    return map(config, descriptor);
  }

  private ObjectNode parseFiles(ObjectMapper mapper, List<File> files, List<String> resources) {
    ObjectNode node = null;
    for (File file : files) {
      try {
        ObjectNode next = (ObjectNode) mapper.readTree(file);
        if (node == null) {
          node = next;
        } else {
          mergeNodes(node, next);
        }
      } catch (IOException e) {
        throw new ConfigurationException("Failed to parse configuration file " + file, e);
      }
    }

    for (String resource : resources) {
      try {
        ObjectNode next = (ObjectNode) mapper.readTree(classLoader.getResource(resource));
        if (node == null) {
          node = next;
        } else {
          mergeNodes(node, next);
        }
      } catch (IOException e) {
        throw new ConfigurationException("Failed to parse configuration resource " + resource, e);
      }
    }
    return node;
  }

  private void mergeNodes(ObjectNode node, ObjectNode parent) {
    Iterator<Map.Entry<String, JsonNode>> iterator = parent.fields();
    while (iterator.hasNext()) {
      Map.Entry<String, JsonNode> entry = iterator.next();
      JsonNode field = node.get(entry.getKey());
      if (field == null) {
        node.set(entry.getKey(), entry.getValue());
      }
      if (field instanceof ObjectNode && entry.getValue() instanceof ObjectNode) {
        mergeNodes((ObjectNode) field, (ObjectNode) entry.getValue());
      }
    }
  }

  /**
   * Loads the given resources using the configuration mapper.
   *
   * @param descriptor the type to load
   * @param resources  the resources to load
   * @param <T>        the resulting type
   * @return the loaded configuration
   */
  public <T> T loadResources(Descriptors.Descriptor descriptor, String... resources) {
    return loadResources(descriptor, Arrays.asList(resources));
  }

  /**
   * Loads the given resources using the configuration mapper.
   *
   * @param descriptor the type to load
   * @param resources  the resources to load
   * @param <T>        the resulting type
   * @return the loaded configuration
   */
  public <T> T loadResources(Descriptors.Descriptor descriptor, List<String> resources) {
    if (resources == null || resources.isEmpty()) {
      throw new IllegalArgumentException("resources must be defined");
    }
    return loadFiles(descriptor, Collections.emptyList(), resources);
  }

  /**
   * Applies the given configuration to the given type.
   *
   * @param config     the configuration to apply
   * @param descriptor the descriptor to which to apply the configuration
   */
  protected <T> T map(ObjectNode config, Descriptors.Descriptor descriptor) {
    return map(config, null, null, descriptor);
  }

  /**
   * Returns a new builder for the given descriptor.
   *
   * @param descriptor the descriptor for which to return the builder
   * @return the builder for the given message descriptor
   */
  protected Message.Builder newBuilder(Descriptors.Descriptor descriptor) {
    Class<?> messageClass = Protos.getMessageClass(descriptor);
    try {
      Method builderMethod = messageClass.getMethod("newBuilder");
      return (Message.Builder) builderMethod.invoke(null);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate builder for message class " + messageClass.getName(), e);
    }
  }

  /**
   * Applies the given configuration to the given type.
   *
   * @param config     the configuration to apply
   * @param descriptor the class to which to apply the configuration
   */
  @SuppressWarnings("unchecked")
  protected <T> T map(ObjectNode config, String path, String name, Descriptors.Descriptor descriptor) {
    Message.Builder builder = newBuilder(descriptor);
    mapFields(builder, descriptor, path, name, config);
    return (T) builder.build();
  }

  private void mapFields(Message.Builder builder, Descriptors.Descriptor descriptor, String path, String name, ObjectNode config) {
    for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
      JsonNode node = config.get(field.getJsonName());
      if (node != null) {
        setValue(builder, field, node, toPath(path, name), field.getJsonName());
      }
    }
  }

  protected void setValue(Message.Builder builder, Descriptors.FieldDescriptor field, JsonNode config, String configPath, String configName) {
    if (field.isMapField()) {
      setMapField(builder, field, config, configPath, configName);
    } else if (field.isRepeated()) {
      setRepeatedField(builder, field, config, configPath, configName);
    } else {
      setValueField(builder, field, config, configPath, configName);
    }
  }

  protected void setRepeatedField(Message.Builder builder, Descriptors.FieldDescriptor field, JsonNode config, String configPath, String configName) {
    if (config.getNodeType() != JsonNodeType.ARRAY) {
      throw new ConfigurationException("Invalid node type for field " + field.getJsonName());
    }

    List<Object> values = new ArrayList<>();
    int i = 0;
    for (JsonNode value : config) {
      values.add(getValueField(field, value, toPath(configPath, configName), String.valueOf(i++)));
    }
    setValue(builder, field, getRepeatedSetterName(field), Iterable.class, values);
  }

  protected void setMapField(Message.Builder builder, Descriptors.FieldDescriptor field, JsonNode config, String configPath, String configName) {
    if (config.getNodeType() != JsonNodeType.OBJECT) {
      throw new ConfigurationException("Invalid node type for field " + field.getJsonName());
    }

    Descriptors.Descriptor descriptor = field.getMessageType();
    Descriptors.FieldDescriptor keyDescriptor = descriptor.findFieldByName("key");
    Descriptors.FieldDescriptor valueDescriptor = descriptor.findFieldByName("value");
    Map<Object, Object> map = new HashMap<>();
    config.fields().forEachRemaining(entry -> {
      Object key = getValueField(keyDescriptor, new TextNode(entry.getKey()), toPath(configPath, configName), entry.getKey());
      Object value = getValueField(valueDescriptor, entry.getValue(), toPath(configPath, configName), entry.getKey());
      map.put(key, value);
    });
    setValue(builder, field, getMapSetterName(field), Map.class, map);
  }

  protected void setValueField(Message.Builder builder, Descriptors.FieldDescriptor field, JsonNode config, String configPath, String configName) {
    Object value = getValueField(field, config, configPath, configName);
    Class<?> type = Primitives.isWrapperType(value.getClass())
        ? Primitives.unwrap(value.getClass())
        : value.getClass();
    setValue(builder, field, getSetterName(field), type, value);
  }

  private void setValue(Message.Builder builder, Descriptors.FieldDescriptor field, String setterName, Class<?> type, Object value) {
    try {
      Method method = builder.getClass().getMethod(setterName, type);
      method.invoke(builder, value);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to set builder field " + field.getFullName(), e);
    }
  }

  protected Object getValueField(Descriptors.FieldDescriptor field, JsonNode node, String configPath, String configPropName) {
    switch (field.getJavaType()) {
      case INT:
        return node.asInt();
      case LONG:
        return node.asLong();
      case FLOAT:
        return (float) node.asDouble();
      case DOUBLE:
        return node.asDouble();
      case BOOLEAN:
        return node.asBoolean();
      case STRING:
        return node.asText();
      case BYTE_STRING:
        return ByteString.copyFrom(BaseEncoding.base64().decode(node.asText()));
      case ENUM:
        return field.getEnumType().findValueByName(node.asText());
      case MESSAGE:
        if (field.getMessageType().getFullName().equals(com.google.protobuf.Duration.getDescriptor().getFullName())) {
          return getDuration(node.asText());
        }
        return map((ObjectNode) node, configPath, configPropName, field.getMessageType());
    }
    return field.getDefaultValue();
  }

  private String getSetterName(Descriptors.FieldDescriptor field) {
    return "set" + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.getJsonName());
  }

  private String getRepeatedSetterName(Descriptors.FieldDescriptor field) {
    return "addAll" + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.getJsonName());
  }

  private String getMapSetterName(Descriptors.FieldDescriptor field) {
    return "putAll" + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.getJsonName());
  }

  private com.google.protobuf.Duration getDuration(String value) {
    if (Strings.isNullOrEmpty(value)) {
      return com.google.protobuf.Duration.newBuilder().build();
    }
    Duration duration = Duration.parse(value);
    return com.google.protobuf.Duration.newBuilder()
        .setSeconds(duration.getSeconds())
        .setNanos(duration.getNano())
        .build();
  }

  protected String toPath(String path, String name) {
    return path != null ? String.format("%s.%s", path, name) : name;
  }
}
