package com.coolhand.kafka;

import com.coolhand.kafka.validators.BatchSizeValidator;
import com.coolhand.kafka.validators.TimeStampValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;


public class MySourceConnectorConfig extends AbstractConfig {

  public static final String OWNER_CONFIG = "github.owner";
  private static final String OWNER_DOC = "Owner of the repository you'd like to follow";
  public static final String BATCH_SIZE_CONFIG="batch.size";
  private static final String BATCH_SIZE_DOC="Number of data points to retrieve at time.";
  public static final String TOPIC_CONFIG="github_topic";
  private static  final String TOPIC_DOC="topic to write ";
  public static final String REPO_CONFIG="github.repo";
  private static final String REPO_DOC="Repository you'd like to follow";
  public static final String SINCE_CONFIG="since.timestamp";

  private static final String SINCE_DOC="Only issues updated at or after this time are returned." +
          "\this is a timestamp in ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ." +
          "Defaults to a year form first launch";
  public static final String AUTH_USERNAME_CONFIG="auth.username";
  private static final String AUTH_USERNAME_DOC="Optional username to authenticate calls";
  public static final String AUTH_PASSWORD_CONFIG="auth.password";
  private static final String AUTH_PASSWORD_DOC="Optional password to authenticate calls";
  public MySourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public MySourceConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {

    return new ConfigDef()
        .define(OWNER_CONFIG, Type.STRING, Importance.HIGH, OWNER_DOC)
            .define(BATCH_SIZE_CONFIG,Type.INT,100,new BatchSizeValidator(),Importance.LOW,BATCH_SIZE_DOC)
            .define(TOPIC_CONFIG,Type.STRING,Importance.HIGH,TOPIC_DOC)
            .define(REPO_CONFIG,Type.STRING,Importance.HIGH,REPO_DOC)
            .define(SINCE_CONFIG,Type.STRING, ZonedDateTime.now().minusYears(1).toInstant().toString(),new TimeStampValidator(), Importance.HIGH,SINCE_DOC)
            .define(AUTH_USERNAME_CONFIG,Type.STRING,"" ,Importance.HIGH,AUTH_USERNAME_DOC)
            .define(AUTH_PASSWORD_CONFIG,Type.STRING,"",Importance.HIGH,AUTH_PASSWORD_DOC);

  }

  public String getOwnerConfig() {
    return this.getString(OWNER_CONFIG);
  }

  public String getTopic() {
    return this.getString(TOPIC_CONFIG);
  }

  public String getRepoConfig() {
    return this.getString(REPO_CONFIG);
  }

  public Integer getBatchSize() {
    return this.getInt(BATCH_SIZE_CONFIG);
  }

  public Instant getSince() {
    return Instant.parse(this.getString(SINCE_CONFIG));
  }

  public String getAuthUserName() {
    return this.getString(AUTH_USERNAME_CONFIG);
  }

  public String getAuthPassword() {
    return this.getString(AUTH_PASSWORD_CONFIG);
  }
}
