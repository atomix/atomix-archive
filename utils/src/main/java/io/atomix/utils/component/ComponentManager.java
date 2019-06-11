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
package io.atomix.utils.component;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.graph.ElementOrder;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.MutableGraph;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component manager.
 */
public class ComponentManager<M> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ComponentManager.class);
  private final Class<M> rootClass;
  private final ClassLoader classLoader;
  private final Map<Class, List<ComponentInstance>> components = new HashMap<>();
  private final ThreadContext threadContext = new SingleThreadContext("atomix-component-manager");
  private MutableGraph<ComponentInstance> dependencyGraph;
  private final AtomicBoolean prepared = new AtomicBoolean();

  public ComponentManager(Class<M> root) {
    this(root, ComponentManager.class.getClassLoader());
  }

  public ComponentManager(Class<M> root, ClassLoader classLoader) {
    this.rootClass = root;
    this.classLoader = classLoader;
  }

  /**
   * Starts the root component.
   *
   * @return a future to be completed with the root component instance
   */
  public CompletableFuture<M> start() {
    if (!prepared.compareAndSet(false, true)) {
      return Futures.exceptionalFuture(new IllegalStateException("Component manager is already running"));
    }

    ComponentInstance<M> instance = new ComponentInstance<>(
        rootClass,
        rootClass.getAnnotation(Component.class),
        true);
    M root;
    try {
      prepare(instance);
      root = instance.instance();
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> start(future));
    return future.thenApply(v -> root);
  }

  /**
   * Stops the root component and all dependents.
   *
   * @return a future to be completed once the components have been stopped
   */
  public CompletableFuture<Void> stop() {
    if (!prepared.compareAndSet(true, false)) {
      return Futures.exceptionalFuture(new IllegalStateException("Component manager is not running"));
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> stop(future));
    return future;
  }

  private void prepare(ComponentInstance<M> root) throws Exception {
    final ClassGraph classGraph = new ClassGraph()
        .enableClassInfo()
        .enableAnnotationInfo()
        .addClassLoader(classLoader);

    try (ScanResult result = classGraph.scan()) {
      for (ClassInfo classInfo : result.getClassesWithAnnotation(Component.class.getName())) {
        Class<?> componentClass = classInfo.loadClass();
        Component componentAnnotation = componentClass.getAnnotation(Component.class);

        ComponentInstance<?> componentInstance = new ComponentInstance<>(componentClass, componentAnnotation);
        components.put(componentClass, Lists.newArrayList(componentInstance));

        for (Class<?> componentInterface : getInterfaces(componentClass)) {
          components.compute(componentInterface, (k, oldComponents) -> {
            if (oldComponents == null) {
              return Lists.newArrayList(componentInstance);
            }

            List<ComponentInstance> newComponents = Lists.newArrayList(oldComponents);
            newComponents.add(componentInstance);
            return newComponents;
          });
        }
      }
    }

    dependencyGraph = buildDependencies(root);
  }

  private MutableGraph<ComponentInstance> buildDependencies(ComponentInstance<M> root) throws Exception {
    final MutableGraph<ComponentInstance> dependencyGraph = GraphBuilder.directed()
        .nodeOrder(ElementOrder.unordered())
        .allowsSelfLoops(false)
        .build();
    buildDependencies(root, dependencyGraph);
    if (Graphs.hasCycle(dependencyGraph)) {
      throw new IllegalStateException("Detected cyclic dependency");
    }
    return dependencyGraph;
  }

  private void buildDependencies(ComponentInstance component, MutableGraph<ComponentInstance> graph) throws Exception {
    Class<?> componentClass = component.type();
    while (componentClass != Object.class) {
      for (Field field : componentClass.getDeclaredFields()) {
        Dependency dependency = field.getAnnotation(Dependency.class);
        if (dependency != null) {
          Class<?> dependencyType = dependency.value() != Dependency.None.class ? dependency.value() : field.getType();
          List<ComponentInstance> dependencies = getComponents(dependencyType);
          field.setAccessible(true);
          if (dependency.cardinality() == Cardinality.SINGLE) {
            ComponentInstance instance = dependencies.get(0);
            field.set(component.instance(), instance.instance());
            graph.putEdge(component, instance);
            buildDependencies(instance, graph);
          } else if (dependency.cardinality() == Cardinality.MULTIPLE) {
            ImmutableList.Builder<Object> builder = ImmutableList.builder();
            for (ComponentInstance instance : dependencies) {
              builder.add(instance.instance());
            }
            field.set(component.instance(), builder.build());
            for (ComponentInstance instance : dependencies) {
              graph.putEdge(component, instance);
              buildDependencies(instance, graph);
            }
          }
        }
      }
      componentClass = componentClass.getSuperclass();
    }
  }

  private void start(CompletableFuture<Void> future) {
    start(startPhases().iterator(), future);
  }

  @SuppressWarnings("unchecked")
  private void start(Iterator<List<ComponentInstance>> iterator, CompletableFuture<Void> future) {
    if (iterator.hasNext()) {
      List<ComponentInstance> phase = iterator.next();
      CompletableFuture.allOf(phase.stream().map(component -> {
        try {
          Object instance = component.instance();
          LOGGER.info("Starting component {}", instance);
          component.setStarted(true);
          if (instance instanceof Managed) {
            try {
              return ((Managed) instance).start();
            } catch (Exception e) {
              return Futures.exceptionalFuture(e);
            }
          }
          return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
          return Futures.exceptionalFuture(e);
        }
      }).toArray(CompletableFuture[]::new))
          .whenCompleteAsync((result, error) -> {
            if (error == null) {
              start(iterator, future);
            } else {
              future.completeExceptionally(error);
            }
          }, threadContext);
    } else {
      future.complete(null);
    }
  }

  private void stop(CompletableFuture<Void> future) {
    stop(stopPhases().iterator(), future);
  }

  @SuppressWarnings("unchecked")
  private void stop(Iterator<List<ComponentInstance>> iterator, CompletableFuture<Void> future) {
    if (iterator.hasNext()) {
      List<ComponentInstance> phase = iterator.next();
      CompletableFuture.allOf(phase.stream().map(component -> {
        try {
          Object instance = component.instance();
          LOGGER.info("Stopping component {}", instance);
          if (instance instanceof Managed) {
            return ((Managed) instance).stop()
                .exceptionally(e -> {
                  LOGGER.warn("An error occurred during shutdown", e);
                  return null;
                });
          }
          return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
          return Futures.exceptionalFuture(e);
        }
      }).toArray(CompletableFuture[]::new))
          .whenCompleteAsync((result, error) -> {
            if (error == null) {
              stop(iterator, future);
            } else {
              future.completeExceptionally(error);
            }
          }, threadContext);
    } else {
      future.complete(null);
    }
  }

  /**
   * Returns a list of parallel startup phases.
   *
   * @return a list of parallel startup phases
   */
  private List<List<ComponentInstance>> startPhases() {
    ImmutableGraph<ComponentInstance> graph = ImmutableGraph.copyOf(dependencyGraph);
    List<List<ComponentInstance>> phases = new LinkedList<>();
    while (true) {
      // Iterate through all the components in the graph and add components for which no dependencies still exist.
      List<ComponentInstance> phase = new LinkedList<>();
      for (ComponentInstance component : graph.nodes()) {
        if (graph.successors(component).isEmpty()) {
          if (graph.predecessors(component).isEmpty()) {
            phases.add(Lists.newArrayList(component));
            return phases;
          }
          phase.add(component);
        }
      }
      phases.add(phase);

      // Remove the phase from the graph to update dependencies.
      MutableGraph<ComponentInstance> mutableGraph = Graphs.copyOf(graph);
      for (ComponentInstance component : phase) {
        mutableGraph.removeNode(component);
      }
      graph = ImmutableGraph.copyOf(mutableGraph);
    }
  }

  private List<List<ComponentInstance>> stopPhases() {
    List<List<ComponentInstance>> startPhases = startPhases();
    List<List<ComponentInstance>> stopPhases = new LinkedList<>();
    for (int i = startPhases.size() - 1; i >= 0; i--) {
      List<ComponentInstance> startPhase = startPhases.get(i);
      List<ComponentInstance> stopPhase = new LinkedList<>();
      for (ComponentInstance component : startPhase) {
        if (component.isStarted()) {
          stopPhase.add(component);
        }
      }

      if (!stopPhase.isEmpty()) {
        stopPhases.add(stopPhase);
      }
    }
    return stopPhases;
  }

  private Set<Class> getInterfaces(Class<?> componentClass) {
    Set<Class> componentInterfaces = new HashSet<>();
    for (Class<?> componentInterface : componentClass.getInterfaces()) {
      componentInterfaces.add(componentInterface);
      componentInterfaces.addAll(getInterfaces(componentInterface));
    }
    return componentInterfaces;
  }

  private List<ComponentInstance> getComponents(Class<?> type) {
    return components.get(type);
  }

  /**
   * Singleton component instance.
   */
  private static class ComponentInstance<T> {
    private final Class<T> type;
    private final Component component;
    private final boolean root;
    private volatile T instance;
    private volatile boolean started;

    ComponentInstance(Class<T> type, Component component) {
      this(type, component, false);
    }

    ComponentInstance(Class<T> type, Component component, boolean root) {
      this.type = type;
      this.component = component;
      this.root = root;
    }

    /**
     * Returns whether the component has been started.
     *
     * @return indicates whether the component has been started
     */
    public boolean isStarted() {
      return started;
    }

    /**
     * Sets whether the component has been started.
     *
     * @param started whether the component has been started
     */
    public void setStarted(boolean started) {
      this.started = started;
    }

    /**
     * Returns the component type.
     *
     * @return the component type
     */
    public Class<?> type() {
      return type;
    }

    /**
     * Returns a boolean indicating whether this component is the root.
     *
     * @return indicates whether this component is the root
     */
    public boolean isRoot() {
      return root;
    }

    /**
     * Constructs a new component instance.
     *
     * @return a new component instance
     */
    public synchronized T instance() throws Exception {
      if (instance == null) {
        Constructor<T> constructor = type.getDeclaredConstructor();
        constructor.setAccessible(true);
        instance = constructor.newInstance();
      }
      return instance;
    }

    @Override
    public String toString() {
      return type.getName();
    }
  }
}
