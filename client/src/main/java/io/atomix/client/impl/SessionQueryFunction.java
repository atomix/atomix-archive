package io.atomix.client.impl;

import io.atomix.api.headers.SessionQueryHeader;

@FunctionalInterface
public interface SessionQueryFunction<S, T> extends RequestFunction<S, SessionQueryHeader, T> {
}