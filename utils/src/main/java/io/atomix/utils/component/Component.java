package io.atomix.utils.component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.protobuf.Message;

/**
 * Component annotation.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Component {

  /**
   * The component configuration class.
   */
  Class<? extends Message> value() default ConfigNone.class;

  /**
   * The component scope.
   */
  Scope scope() default Scope.RUNTIME;

  /**
   * Default configuration class.
   */
  abstract class ConfigNone implements Message {
  }

  /**
   * Component scope.
   */
  enum Scope {
    RUNTIME,
    TEST,
  }
}
