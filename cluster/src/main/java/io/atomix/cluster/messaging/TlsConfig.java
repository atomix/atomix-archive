/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging;

/**
 * TLS configuration.
 */
public class TlsConfig {
  private boolean enabled = Boolean.parseBoolean(System.getProperty("io.atomix.enableNettyTLS", Boolean.toString(false)));
  private String certPath = System.getProperty("io.atomix.messaging.tls.certPath");
  private String keyPath = System.getProperty("io.atomix.messaging.tls.keyPath");

  /**
   * Returns whether TLS is enabled.
   *
   * @return indicates whether TLS is enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Sets whether TLS is enabled.
   *
   * @param enabled whether TLS is enabled
   * @return the TLS configuration
   */
  public TlsConfig setEnabled(boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  /**
   * Returns the certificate chain path.
   *
   * @return the certificate chain path
   */
  public String getCertPath() {
    return certPath;
  }

  /**
   * Sets the certificate chain path.
   *
   * @param certPath the certificate chain path
   * @return the TLS configuration
   */
  public TlsConfig setCertPath(String certPath) {
    this.certPath = certPath;
    return this;
  }

  /**
   * Returns the private key path.
   *
   * @return the private key path
   */
  public String getKeyPath() {
    return keyPath;
  }

  /**
   * Sets the private key path.
   *
   * @param keyPath the private key path
   * @return the TLS configuration
   */
  public TlsConfig setKeyPath(String keyPath) {
    this.keyPath = keyPath;
    return this;
  }
}
