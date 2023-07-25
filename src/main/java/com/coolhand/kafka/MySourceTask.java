package com.coolhand.kafka;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.event.WindowStateListener;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MySourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MySourceTask.class);

  public MySourceConnectorConfig config;

  protected Instant nextQuerySince;
  protected Integer lastIssueNumber;
  protected Integer nextPageToVisit=1;
  protected Instant lastUpdatedAt;

  MyAPIHttpClient myAPIHttpClient;


  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
    config=new MySourceConnectorConfig(map);
    initializeLastVariables();
    myAPIHttpClient = new MyAPIHttpClient(config);
  }

  Map<String, Object> lastSourceOffset;
  private void initializeLastVariables() {

    String jsonOffsetData=new Gson().toJson(lastSourceOffset);
    TypeToken<Map<String,Object>> typeToken = new TypeToken<Map<String,Object>>(){};
    Map<String,Object> offsetData = new Gson().fromJson(jsonOffsetData,typeToken.getType());
    if(offsetData==null){
      nextQuerySince=config.getSince();
      lastIssueNumber=-1;
    }else {
      Object updatedAt=offsetData.get(MySchemas.UPDATED_AT_FIELD);
      Object issueNumber=offsetData.get(MySchemas.NUMBER_FIELD);
      Object nextPage=offsetData.get(MySchemas.NEXT_PAGE_FIELD);
      if(updatedAt!=null && (updatedAt instanceof String)){
        nextQuerySince=Instant.parse((String) issueNumber);
      }
      if(issueNumber !=null && (issueNumber instanceof String)){
        lastIssueNumber= Integer.valueOf((String)issueNumber);
      }
      if(nextPage !=null && (nextPage instanceof String)){
        nextPageToVisit=Integer.valueOf((String) nextPage);
      }
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    myAPIHttpClient.sleepIfNeed();
    final ArrayList<SourceRecord> records = new ArrayList<>();
    JSONArray issues = myAPIHttpClient.getNextIssues(nextPageToVisit,nextQuerySince);
    //TODO: Create SourceRecord objects that will be sent the kafka cluster.
    int i=0;
    for(Object obj:issues){
//      com.simplesteph.kafka.model.Issue issue=Issue.fromJson((JSONObject) obj);
    }
    throw new UnsupportedOperationException("This has not been implemented.");
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }
}