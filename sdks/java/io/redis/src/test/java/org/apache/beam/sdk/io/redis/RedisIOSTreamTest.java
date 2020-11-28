package org.apache.beam.sdk.io.redis;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
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
public class RedisIOSTreamTest implements Serializable {
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
    String tempKey = "temp_key";
    String tempGroup = "temp_group";
    List<Map<String, String>> data = buildIncrementalData(100);
    data.stream().forEach(map -> client.xadd(tempKey, StreamEntryID.NEW_ENTRY, map));
    //PCollection<Map<String,String>> pcoll = 
    p.apply("read", RedisStreamIO.read()
        .withEndpoint(REDIS_HOST, port)
        .withKeyPattern(tempKey)
        .withBatchSize(1)
        .withGroupId(tempGroup)
        .withTimeout(1000)
        .withMaxNumRecords(10))
        .apply("write test", new WriteTest()); 

    //PAssert.that(pcoll).containsInAnyOrder(data);
    p.run();

  }
  public static class WriteTest extends PTransform<PCollection<StreamEntry>, PDone> {

    @Override
    public PDone expand(PCollection<StreamEntry> input) {
      return input.apply("map to strings", MapElements.into(TypeDescriptors.strings()).via(se -> fieldToString(se)))
      .apply("window", Window.into(FixedWindows.of(org.joda.time.Duration.standardSeconds(1))))
      .apply("write", TextIO.write().to("LocalDir/").withWindowedWrites().withNumShards(1));
    }

    public String fieldToString(StreamEntry se) {
      return se.getFields().values().stream().collect(Collectors.joining(","));
    } 
  }

  public TypeDescriptor<Map<String,String>> mapDescriptor() {
    return TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.strings());
  }

  public String mapToString(Map<String,String> map) {
    return map.values().stream().collect(Collectors.joining(","));
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
