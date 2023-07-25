package com.coolhand.kafka;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.coolhand.kafka.MySourceConnectorConfig.*;
import static org.junit.Assert.*;

public class MySourceConnectorTest {

  private Map<String,String> initialConfig(){
    Map<String,String> baseProps=new HashMap<>();
    baseProps.put(OWNER_CONFIG,"foo");
    baseProps.put(REPO_CONFIG,"bar");
    baseProps.put(SINCE_CONFIG, "2017-04-26T01:23:45Z");
    baseProps.put(BATCH_SIZE_CONFIG, "100");
    baseProps.put(TOPIC_CONFIG, "github-issues");
    return baseProps;
  }

  @Test
  public void testTaskConfigREturnOneTaskConfig() {
    // Congrats on a passing test!
    MySourceConnector mySourceConnector=new MySourceConnector();
    mySourceConnector.start(initialConfig());
    assertEquals(mySourceConnector.taskConfigs(1).size(),1);
    assertEquals(mySourceConnector.taskConfigs(10).size(),1);
  }
}
