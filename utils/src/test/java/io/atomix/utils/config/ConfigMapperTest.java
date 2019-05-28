package io.atomix.utils.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Config mapper test.
 */
public class ConfigMapperTest {
  @Test
  public void testConfigMapper() throws Exception {
    ConfigMapper mapper = new ConfigMapper(ClassLoader.getSystemClassLoader());
    TestConfig config = mapper.loadResources(TestConfig.getDescriptor(), "test_config.yaml");
    assertEquals(1, config.getSomeInt());
    assertTrue(config.getSomeBool());
    assertEquals("Hello world!", config.getSomeText());
    assertEquals("Hello world again!", config.getSomeSubConfig().getSomeText());
    assertEquals("one", config.getSomeTexts(0));
    assertEquals("two", config.getSomeTexts(1));
    assertEquals("three", config.getSomeTexts(2));
    assertEquals("one", config.getSomeSubConfigs(0).getSomeText());
    assertEquals("two", config.getSomeSubConfigs(1).getSomeText());
    assertEquals("three", config.getSomeSubConfigs(2).getSomeText());
    assertEquals("two", config.getSomeMapTextsOrThrow("one"));
    assertEquals("four", config.getSomeMapTextsOrThrow("three"));
    assertEquals("two", config.getSomeMapConfigsOrThrow("one").getSomeText());
    assertEquals("four", config.getSomeMapConfigsOrThrow("three").getSomeText());
  }
}
