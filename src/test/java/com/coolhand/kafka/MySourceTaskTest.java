package com.coolhand.kafka;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;
import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.coolhand.kafka.MySourceConnectorConfig.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MySourceTaskTest {


  private MySourceTask mySourceTask=new MySourceTask();
  private Integer batchSize=10;

  private Map<String , String > initialConfig(){
    Map<String ,String> baseProps = new HashMap<>();
    baseProps.put(OWNER_CONFIG,"apache");
    baseProps.put(REPO_CONFIG,"kafka");
    baseProps.put(SINCE_CONFIG,"2017-04-26T01:23:45Z");
    baseProps.put(BATCH_SIZE_CONFIG,batchSize.toString());
    baseProps.put(TOPIC_CONFIG,"github-issues");
    return  baseProps;
  }

  @Test
  public void test() throws UnirestException {

    mySourceTask.config=new MySourceConnectorConfig(initialConfig());
    mySourceTask.nextPageToVisit=1;
    mySourceTask.nextQuerySince= Instant.parse("2017-04-26T01:23:45Z");
    mySourceTask.myAPIHttpClient=new MyAPIHttpClient(mySourceTask.config);
    String url = mySourceTask.myAPIHttpClient.constructUrl(mySourceTask.nextPageToVisit,mySourceTask.nextQuerySince);
    System.out.println(url);
    HttpResponse<JsonNode> httpResponse = mySourceTask.myAPIHttpClient.getNextIssuesAPI(mySourceTask.nextPageToVisit,mySourceTask.nextQuerySince);
    if(httpResponse.getStatus() !=403){
      assertEquals(200,httpResponse.getStatus());
      Set<String > headers=httpResponse.getHeaders().keySet();
      assertTrue(headers.contains(MyAPIHttpClient.X_RATELIMI_LIMIT_HEADER));
//      assertTrue(headers.contains(MyAPIHttpClient.X_RATELIMIT_REMAINING_HEADER));
//      assertTrue(headers.contains(MyAPIHttpClient.X_RATELIMIT_RESET_HEADER));

      assertEquals(batchSize.intValue(),httpResponse.getBody().getArray().length());

      JSONObject jsonObject = (JSONObject) httpResponse.getBody().getArray().get(0);
//      Issue issue = Issue.fromJson(jsonObject);



    }

  }
}