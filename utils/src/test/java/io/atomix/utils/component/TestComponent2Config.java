package io.atomix.utils.component;

import io.atomix.utils.config.Config;

/**
 * Test component configuration.
 */
public class TestComponent2Config implements Config {
  private String value = "2";

  public String getValue() {
    return value;
  }
}
