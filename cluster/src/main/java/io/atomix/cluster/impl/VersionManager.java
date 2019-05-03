package io.atomix.cluster.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.google.common.io.Resources;
import io.atomix.cluster.VersionService;
import io.atomix.utils.Version;
import io.atomix.utils.component.Component;
import io.atomix.utils.config.ConfigurationException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Version service implementation.
 */
@Component
public class VersionManager implements VersionService {

  private static final String VERSION_RESOURCE = "VERSION";

  private static final String BUILD;
  private static final Version VERSION;

  static {
    try {
      BUILD = Resources.toString(checkNotNull(VersionManager.class.getClassLoader().getResource(VERSION_RESOURCE),
          VERSION_RESOURCE + " resource is null"), StandardCharsets.UTF_8);
    } catch (IOException | NullPointerException e) {
      throw new ConfigurationException("Failed to load Atomix version", e);
    }
    VERSION = BUILD.trim().length() > 0 ? Version.from(BUILD.trim().split("\\s+")[0]) : null;
  }

  private final Version version;

  public VersionManager() {
    this.version = VERSION;
  }

  public VersionManager(Version version) {
    this.version = version;
  }

  @Override
  public Version version() {
    return version;
  }
}
