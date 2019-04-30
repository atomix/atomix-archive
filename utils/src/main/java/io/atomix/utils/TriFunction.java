package io.atomix.utils;

/**
 * Three argument function.
 */
@FunctionalInterface
public interface TriFunction<T, U, V, W> {
  W apply(T arg1, U arg2, V arg3);
}
