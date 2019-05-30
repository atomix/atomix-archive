package io.atomix.client.impl;

import io.atomix.api.headers.SessionCommandHeader;

@FunctionalInterface
public interface SessionCommandFunction<S, T> extends RequestFunction<S, SessionCommandHeader, T> {
}