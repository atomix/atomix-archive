package io.atomix.utils.config;

import io.atomix.utils.ConfiguredType;

/**
 * Type registry.
 */
public interface TypeRegistry<T extends ConfiguredType> {

  /**
   * Returns the type with the given name.
   *
   * @param name the type name
   * @return the type with the given name
   */
  T getType(String name);

}
