package io.atomix.client;

import io.atomix.client.channel.ChannelConfig;
import io.atomix.client.channel.ChannelProvider;
import io.atomix.client.channel.ServerChannelProvider;
import io.atomix.client.channel.ServiceChannelProvider;
import io.atomix.utils.Builder;
import io.atomix.utils.net.Address;

/**
 * Atomix client builder.
 */
public class AtomixClientBuilder implements Builder<AtomixClient> {
  private ChannelProvider channelProvider;
  private final ChannelConfig channelConfig = new ChannelConfig();

  /**
   * Sets the server to which to connect.
   *
   * @param host the server host
   * @param port the server port
   * @return the client builder
   */
  public AtomixClientBuilder withServer(String host, int port) {
    return withServer(Address.from(host, port));
  }

  /**
   * Sets the server to which to connect.
   *
   * @param address the server address
   * @return the client builder
   */
  public AtomixClientBuilder withServer(Address address) {
    this.channelProvider = new ServerChannelProvider(address, channelConfig);
    return this;
  }

  /**
   * Sets the service to which to connect.
   *
   * @param serviceName the service name
   * @return the client builder
   */
  public AtomixClientBuilder withServiceName(String serviceName) {
    this.channelProvider = new ServiceChannelProvider(serviceName, channelConfig);
    return this;
  }

  /**
   * Enables TLS.
   *
   * @return the client builder
   */
  public AtomixClientBuilder withTlsEnabled() {
    return withTlsEnabled(true);
  }

  /**
   * Sets whether TLS is enabled.
   *
   * @param tlsEnabled whether to enable TLS
   * @return the client builder
   */
  public AtomixClientBuilder withTlsEnabled(boolean tlsEnabled) {
    channelConfig.setTlsEnabled(tlsEnabled);
    return this;
  }

  /**
   * Sets the client key path.
   *
   * @param keyPath the client key path
   * @return the client builder
   */
  public AtomixClientBuilder withKeyPath(String keyPath) {
    channelConfig.setKeyPath(keyPath);
    return this;
  }

  /**
   * Sets the client cert path.
   *
   * @param certPath the client cert path
   * @return the client builder
   */
  public AtomixClientBuilder withCertPath(String certPath) {
    channelConfig.setCertPath(certPath);
    return this;
  }

  @Override
  public AtomixClient build() {
    return new AtomixClient(channelProvider);
  }
}
