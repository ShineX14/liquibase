package org.liquibase.maven.plugins.spi;

public interface PropertyDecrypter {

  String decrypt(String key, String value);

}
