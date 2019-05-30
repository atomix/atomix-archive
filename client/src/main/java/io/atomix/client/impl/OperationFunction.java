package io.atomix.client.impl;

import io.atomix.api.headers.RequestHeader;

@FunctionalInterface
public interface OperationFunction<S, T> extends RequestFunction<S, RequestHeader, T> {
}