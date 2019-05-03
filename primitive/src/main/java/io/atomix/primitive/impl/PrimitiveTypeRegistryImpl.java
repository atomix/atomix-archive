package io.atomix.primitive.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.utils.component.Cardinality;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Primitive type registry.
 */
@Component
public class PrimitiveTypeRegistryImpl implements PrimitiveTypeRegistry, Managed {

  @Dependency(value = PrimitiveType.class, cardinality = Cardinality.MULTIPLE)
  private List<PrimitiveType> types;

  private final Map<String, PrimitiveType> typeMap = new ConcurrentHashMap<>();

  @Override
  public Collection<PrimitiveType> getPrimitiveTypes() {
    return types;
  }

  @Override
  public PrimitiveType getPrimitiveType(String typeName) {
    return typeMap.get(typeName);
  }

  @Override
  public CompletableFuture<Void> start() {
    types.forEach(type -> typeMap.put(type.name(), type));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }
}
