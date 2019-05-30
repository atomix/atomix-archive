package io.atomix.client.impl;

import io.atomix.api.headers.SessionHeader;

@FunctionalInterface
public interface SessionFunction<S, T> extends RequestFunction<S, SessionHeader, T> {
}