package io.atomix.utils.component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Dependency annotation.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Dependency {

  /**
   * The dependency type.
   */
  Class<?> value() default None.class;

  /**
   * The dependency cardinality.
   */
  Cardinality cardinality() default Cardinality.SINGLE;

  class None {
  }
}
