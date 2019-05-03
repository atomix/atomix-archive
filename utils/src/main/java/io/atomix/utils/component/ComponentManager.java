package io.atomix.utils.component;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.CaseFormat;
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
import io.atomix.utils.config.Config;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component manager.
 */
public class ComponentManager<C extends Config, M> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ComponentManager.class);
  private final Class<M> rootClass;
  private final ClassLoader classLoader;
  private final Component.Scope scope;
  private final Map<Class, List<ComponentInstance>> components = new HashMap<>();
  private final ThreadContext threadContext = new SingleThreadContext("atomix-component-manager");
  private MutableGraph<ComponentInstance> dependencyGraph;
  private final AtomicBoolean prepared = new AtomicBoolean();

  public ComponentManager(Class<M> root) {
    this(root, ComponentManager.class.getClassLoader(), Component.Scope.RUNTIME);
  }

  public ComponentManager(Class<M> root, ClassLoader classLoader, Component.Scope scope) {
    this.rootClass = root;
    this.classLoader = classLoader;
    this.scope = scope;
  }

  /**
   * Starts the root component.
   *
   * @param config the component configuration
   * @return a future to be completed with the root component instance
   */
  public CompletableFuture<M> start(C config) {
    if (!prepared.compareAndSet(false, true)) {
      return Futures.exceptionalFuture(new IllegalStateException("Component manager is already running"));
    }

    ComponentInstance<M> instance = new ComponentInstance<>(
        rootClass,
        rootClass.getAnnotation(Component.class),
        config,
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
    final ConfigMap configMap = new ConfigMap(root.config());

    final ClassGraph classGraph = new ClassGraph()
        .enableClassInfo()
        .enableAnnotationInfo()
        .addClassLoader(classLoader);

    try (ScanResult result = classGraph.scan()) {
      for (ClassInfo classInfo : result.getClassesWithAnnotation(Component.class.getName())) {
        Class<?> componentClass = classInfo.loadClass();
        Component componentAnnotation = componentClass.getAnnotation(Component.class);
        Config componentConfig = componentAnnotation.value() != Component.ConfigNone.class
            ? configMap.getConfig(componentAnnotation.value())
            : null;

        ComponentInstance<?> componentInstance = new ComponentInstance<>(componentClass, componentAnnotation, componentConfig);
        components.put(componentClass, Lists.newArrayList(componentInstance));

        for (Class<?> componentInterface : getInterfaces(componentClass)) {
          components.compute(componentInterface, (k, oldComponents) -> {
            if (oldComponents == null) {
              return Lists.newArrayList(componentInstance);
            }

            for (int i = 0; i < oldComponents.size(); i++) {
              ComponentInstance oldComponent = oldComponents.get(i);
              if (!oldComponent.isScope(scope) && componentInstance.isScope(scope)) {
                List<ComponentInstance> newComponents = Lists.newArrayList(componentInstance);
                newComponents.addAll(oldComponents);
                return newComponents;
              } else if (!oldComponent.isConfigurable() && componentInstance.isConfigurable() && componentInstance.isConfigured()) {
                List<ComponentInstance> newComponents = Lists.newArrayList(componentInstance);
                newComponents.addAll(oldComponents);
                return newComponents;
              } else if (oldComponent.isConfigurable() && componentInstance.isConfigurable() && !oldComponent.isConfigured() && componentInstance.isConfigured()) {
                List<ComponentInstance> newComponents = Lists.newArrayList(componentInstance);
                newComponents.addAll(oldComponents);
                return newComponents;
              }
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
          if (instance instanceof Managed) {
            return ((Managed) instance).start(component.config());
          }
          component.setStarted(true);
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
    private final Config config;
    private final boolean root;
    private volatile T instance;
    private volatile boolean started;

    ComponentInstance(Class<T> type, Component component, Config config) {
      this(type, component, config, false);
    }

    ComponentInstance(Class<T> type, Component component, Config config, boolean root) {
      this.type = type;
      this.component = component;
      this.config = config;
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
     * Returns a boolean indicating whether the component is in the given scope.
     *
     * @param scope the scope to check
     * @return indicates whether the component is in the given scope
     */
    public boolean isScope(Component.Scope scope) {
      return scope == component.scope();
    }

    /**
     * Returns a boolean indicating whether the component is configurable.
     *
     * @return indicates whether the component is configurable
     */
    public boolean isConfigurable() {
      return component.value() != Component.ConfigNone.class;
    }

    /**
     * Returns a boolean indicating whether the component is configured.
     *
     * @return indicates whether the component is configured
     */
    public boolean isConfigured() {
      return config != null;
    }

    /**
     * Returns the component configuration.
     *
     * @return the component configuration
     */
    public Config config() {
      return config;
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

  private static class ConfigMap {
    private final Map<Class, Config> configs = new HashMap<>();

    ConfigMap(Config config) throws Exception {
      mapConfigs(config);
    }

    /**
     * Returns a configuration value of the given type.
     *
     * @param type the configuration type
     * @param <T>  the configuration type
     * @return the configuration object
     */
    @SuppressWarnings("unchecked")
    public <T extends Config> T getConfig(Class<T> type) {
      return (T) configs.get(type);
    }

    private void mapConfigs(Config config) throws Exception {
      if (config != null) {
        Class<?> configClass = config.getClass();
        while (configClass != Object.class) {
          configs.putIfAbsent(configClass, config);
          mapConfigs(configClass, config);
          configClass = configClass.getSuperclass();
        }
      }
    }

    private void mapConfigs(Class<?> configClass, Config config) throws Exception {
      // Iterate through methods first to find getters for configuration classes.
      for (Method method : configClass.getDeclaredMethods()) {
        if (Modifier.isStatic(method.getModifiers())) {
          continue;
        }

        // If this is a getter method, get the configuration and map it.
        if (method.getName().startsWith("get")
            && method.getParameterCount() == 0
            && Config.class.isAssignableFrom(method.getReturnType())) {
          mapConfigs((Config) method.invoke(config));
        }
      }

      for (Field field : configClass.getDeclaredFields()) {
        if (Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
          continue;
        }

        if (Config.class.isAssignableFrom(field.getType())) {
          try {
            configClass.getMethod(getterName(field));
          } catch (NoSuchMethodException e) {
            field.setAccessible(true);
            mapConfigs((Config) field.get(config));
          }
        }
      }
    }

    private static String getterName(Field field) {
      return "get" + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, field.getName());
    }
  }
}
