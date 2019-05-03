package io.atomix.cluster;

import io.atomix.utils.Version;

/**
 * Version service.
 */
public interface VersionService {

  /**
   * Returns the software version.
   *
   * @return the software version
   */
  Version version();

}
