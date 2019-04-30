package io.atomix.utils;

/**
 * Four argument function.
 */
@FunctionalInterface
public interface QuadFunction<T, U, V, W, X> {
  X apply(T arg1, U arg2, V arg3, W arg4);
}
