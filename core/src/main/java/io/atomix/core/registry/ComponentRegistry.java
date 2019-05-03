package io.atomix.core.registry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.core.AtomixRegistry;
import io.atomix.utils.ConfiguredType;
import io.atomix.utils.NamedType;
import io.atomix.utils.component.Cardinality;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Component based type registry.
 */
@Component
public class ComponentRegistry implements AtomixRegistry, Managed {

  @Dependency(value = NamedType.class, cardinality = Cardinality.MULTIPLE)
  private List<NamedType> types;
  private final Map<Class<? extends NamedType>, Map<String, NamedType>> registrations = new ConcurrentHashMap<>();

  @Override
  @SuppressWarnings("unchecked")
  public <T extends NamedType> Collection<T> getTypes(Class<T> type) {
    Map<String, NamedType> types = registrations.get(type);
    return types != null ? (Collection<T>) types.values() : Collections.emptyList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends NamedType> T getType(Class<T> type, String name) {
    Map<String, NamedType> types = registrations.get(type);
    return types != null ? (T) types.get(name) : null;
  }

  private Class<? extends NamedType> getClassType(Class<?> type) {
    while (type != Object.class) {
      Class<? extends NamedType> baseType = getInterfaceType(type);
      if (baseType != null) {
        return baseType;
      }
      type = type.getSuperclass();
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Class<? extends NamedType> getInterfaceType(Class<?> type) {
    for (Class<?> iface : type.getInterfaces()) {
      if (iface == ConfiguredType.class || iface == NamedType.class) {
        return (Class<? extends NamedType>) type;
      }
    }
    for (Class<?> iface : type.getInterfaces()) {
      type = getInterfaceType(iface);
      if (type != null) {
        return (Class<? extends NamedType>) type;
      }
    }
    return null;
  }

  @Override
  public CompletableFuture<Void> start() {
    for (NamedType type : types) {
      registrations.computeIfAbsent(
          getClassType(type.getClass()),
          t -> new ConcurrentHashMap<>()).put(type.name(), type);
    }
    return CompletableFuture.completedFuture(null);
  }
}
