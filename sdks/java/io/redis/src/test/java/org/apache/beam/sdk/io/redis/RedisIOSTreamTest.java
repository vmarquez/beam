package org.apache.beam.sdk.io.redis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.embedded.RedisServer;

/** Test on the Redis IO. */
@RunWith(JUnit4.class)
public class RedisIOSTreamTest{
  private static final String REDIS_HOST = "localhost";

  private static final String REDIS_KEY = "key_" + UUID.randomUUID().toString().substring(0, 7);

  private static final String REDIS_GROUPNAME = "group_" + UUID.randomUUID().toString().substring(0, 7);

  @Rule
  public TestPipeline p = TestPipeline.create();

  private static RedisServer server;
  private static int port = 6379;

  private static Jedis client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    //port = NetworkTestHelper.getAvailableLocalPort();
    //server = new RedisServer(port);
    //server.start();
    client = RedisConnectionConfiguration.create(REDIS_HOST, port).connect();
  }

  @AfterClass
  public static void afterClass() {
    client.close();
    //server.stop();
  }

  @Test
  public void testReadStream() {
    List<Map<String, String>> data = buildIncrementalData(10);
    data.stream().forEach(map -> client.xadd(REDIS_KEY, StreamEntryID.NEW_ENTRY, map));
    PCollection<Map<String,String>> pcoll = p.apply("read", RedisStreamIO.read()
        .withEndpoint(REDIS_HOST, port)
        .withKeyPattern(REDIS_KEY)
        .withBatchSize(1)
        .withGroupId(REDIS_GROUPNAME)
        .withTimeout(1000)
        .withMaxNumRecords(10))
        .apply("map to map", MapElements.into(mapDescriptor()).via(se -> se.getFields()));


    PAssert.that(pcoll).containsInAnyOrder(data);
    p.run();

  }

  public TypeDescriptor<Map<String,String>> mapDescriptor() {
    return TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.strings());
  }

  private List<Map<String, String>> buildIncrementalData(int size) {
    List<Map<String, String>> data = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      Map<String, String> map = new HashMap<>();
      map.put("a", i + "");
      data.add(map);
    }
    return data;
  }

}
