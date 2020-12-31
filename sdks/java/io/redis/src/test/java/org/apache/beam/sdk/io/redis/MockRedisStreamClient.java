package org.apache.beam.sdk.io.redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.time.Instant;

import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class MockRedisStreamClient {


  public Map<String, List<StreamEntry>> streams = new HashMap<>();

  public Map<String, List<StreamEntry>> pending = new HashMap<>();

  public Map<String, Integer> lastRead = new HashMap<>();

  //TreeMap<Long, List<Map<String<String>>> 
  public void xadd(String key, StreamEntryID id, Map<String, String> data) {
    //generate new streamEntry ID if new entry?
    //generate time
    
    streams.get(key).add(new StreamEntry(id, data));
    return;
  }


  public static class RedisStreamData {

    public StreamEntryID lastID = new StreamEntryID();
    
    TreeMap<Long, List<Map<String, String>>> queue = new TreeMap<>();

    public StreamEntryID add(StreamEntryID id, Map<String, String> data) {
      return null;
    }

    public StreamEntry getNext(StreamEntryID lastRead) {
      List<Map<String, String>> list = queue.get(lastRead.getTime());
      if (list.size() > lastRead.getSequence()) {
        return new StreamEntry(new StreamEntryID(lastRead.getTime(), lastRead.getSequence()+1), list.get((int)lastRead.getSequence()+1));
      } else {
        //get the higher list and boom there
       long time = queue.higherEntry(lastRead.getTime()).getKey();
       return new StreamEntry(new StreamEntryID(time, 0), queue.get(time).get(0));
      }
   }

    //Not thread safe
    public StreamEntryID add(Map<String, String> data) {
      long nowSecond = Instant.now().getEpochSecond();
      if(!queue.isEmpty() && nowSecond == queue.lastKey()) {
        queue.lastEntry().getValue().add(data);
        return new StreamEntryID(nowSecond, queue.lastEntry().getValue().size());
      } else {
        queue.put(nowSecond, new ArrayList<>(Collections.singleton(data)));
        return new StreamEntryID(nowSecond, 0); 
      }
    }
  }
}