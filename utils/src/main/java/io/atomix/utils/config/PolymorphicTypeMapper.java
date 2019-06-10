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
package io.atomix.utils.config;

import com.google.protobuf.Descriptors;
import io.atomix.utils.ConfiguredType;

/**
 * Polymorphic type mapper.
 */
public class PolymorphicTypeMapper {
  private final Descriptors.Descriptor containerDescriptor;
  private final Descriptors.FieldDescriptor nameField;
  private final Descriptors.FieldDescriptor anyField;
  private final TypeRegistry registry;

  public PolymorphicTypeMapper(
      Descriptors.Descriptor containerDescriptor,
      Descriptors.FieldDescriptor nameField,
      Descriptors.FieldDescriptor anyField,
      TypeRegistry registry) {
    this.containerDescriptor = containerDescriptor;
    this.nameField = nameField;
    this.anyField = anyField;
    this.registry = registry;
  }

  /**
   * Returns the descriptor for the type name field.
   *
   * @return the descriptor for the type name field
   */
  public Descriptors.FieldDescriptor getNameField() {
    return nameField;
  }

  /**
   * Returns the descriptor for the any field on which to set the configuration.
   *
   * @return the descriptor for the any field on which to set the descriptor
   */
  public Descriptors.FieldDescriptor getAnyField() {
    return anyField;
  }

  /**
   * Returns the descriptor for the containing type.
   *
   * @return the descriptor for the containing type
   */
  public Descriptors.Descriptor getContainerDescriptor() {
    return containerDescriptor;
  }

  /**
   * Returns the descriptor for the type's configuration.
   *
   * @param name the type name
   * @return the descriptor for the given type's configuration
   */
  public Descriptors.Descriptor getConfigDescriptor(String name) {
    ConfiguredType type = registry.getType(name);
    if (type == null) {
      return null;
    }
    return type.getConfigDescriptor();
  }
}
