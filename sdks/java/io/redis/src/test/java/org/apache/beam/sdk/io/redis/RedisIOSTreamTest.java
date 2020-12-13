package org.apache.beam.sdk.io.redis;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.redis.RedisStreamIO.ReadStream;
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
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.DescriptorProtos.UninterpretedOption;
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
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class RedisIOSTreamTest implements Serializable {
  private static final String REDIS_HOST = "localhost";

  private static final String REDIS_KEY = "key_" + UUID.randomUUID().toString().substring(0, 7);

  private static final String REDIS_GROUPNAME = "group_" + UUID.randomUUID().toString().substring(0, 7);

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Rule
  public TestPipeline p2 = TestPipeline.create(); 

  private static RedisServer server;
  private static int port = 6379;

  private static Jedis client = null;

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
  public void testReadStream() throws InterruptedException, IOException {
    String tempKey = "temp_key";
    String tempGroup = "temp_group";
    List<Map<String, String>> data = buildIncrementalData(0, 10);
    System.out.println("list size = " + data.size()); 
    data.stream().forEach(map -> client.xadd(REDIS_KEY, StreamEntryID.NEW_ENTRY, map));
    PCollection<String> pcoll = p.apply("read", getReadStream(REDIS_KEY, 10))
        .apply("Map", MapElements.into(TypeDescriptors.strings()).via(se -> fieldToString(se)));

    PAssert.that(pcoll).containsInAnyOrder(data.stream().map(m -> "a," + m.get("a")).collect(Collectors.toList()));

    PipelineResult readResult = p.run();
    PipelineResult.State readState =
        readResult.waitUntilFinish(org.joda.time.Duration.standardSeconds(4));
  }


  /*
  * This test will read half of a queue, then finish the pipeline, 
  * then should be able to run a second pipeline picking back from where it left off
  */
  @Test
  public void testReadStreamAfterRestart() throws InterruptedException, IOException {
    String newKey = "temp_key";
    String tempGroup = "temp_group";
    List<Map<String, String>> data1 = buildIncrementalData(0, 5);

    List<Map<String, String>> data2 = buildIncrementalData(5, 5);
    data1.stream().forEach(map -> client.xadd(REDIS_KEY, StreamEntryID.NEW_ENTRY, map));
    data2.stream().forEach(map -> client.xadd(REDIS_KEY, StreamEntryID.NEW_ENTRY, map));

    data2.stream().forEach(map -> System.out.println("data2 = " + map.toString()));

    PCollection<String> pcoll = p.apply("read", getReadStream(newKey, 5))
      .apply("Map", MapElements.into(TypeDescriptors.strings()).via(se -> fieldToString(se)));

    PAssert.that(pcoll).containsInAnyOrder(data1.stream().map(m -> "a," + m.get("a")).collect(Collectors.toList()));

    PipelineResult rr = p.run();
    
    rr.waitUntilFinish(org.joda.time.Duration.standardSeconds(4));

    PCollection<String> pcoll2 = p2.apply("read again", getReadStream(newKey, 5))
      .apply("Map", MapElements.into(TypeDescriptors.strings()).via(se -> fieldToString(se)));
    
    PAssert.that(pcoll2).containsInAnyOrder(data2.stream().map(m -> "a," + m.get("a")).collect(Collectors.toList()));

    PipelineResult rr2 = p2.run();
    rr2.waitUntilFinish(org.joda.time.Duration.standardSeconds(4));
  }

  public ReadStream getReadStream(String key, long maxNumRecords) {
    return RedisStreamIO.read()
        .withEndpoint(REDIS_HOST, port)
        .withKeyPattern(key)
        .withBatchSize(5)
        .withGroupId("group_id")
        .withTimeout(1000)
        .withMaxNumRecords(maxNumRecords);
  }

  public static String fieldToString(StreamEntry se) {
    String str =  se.getFields().values().stream().collect(Collectors.joining(","));
    return "a," + str;
    }

  public static class WriteTest extends PTransform<PCollection<StreamEntry>, PDone> {

    @Override
    public PDone expand(PCollection<StreamEntry> input) {
      return input.apply("map to strings", MapElements.into(TypeDescriptors.strings()).via(se -> fieldToString(se)))
      .apply("window", Window.into(FixedWindows.of(org.joda.time.Duration.standardSeconds(1))))
      .apply("write", TextIO.write().to("/Users/vincentmarquez/RedisStreamTest/").withWindowedWrites().withNumShards(1));
    }
  }

  public TypeDescriptor<Map<String,String>> mapDescriptor() {
    return TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.strings());
  }

  public String mapToString(Map<String,String> map) {
    return map.values().stream().collect(Collectors.joining(","));
  }

  private List<Map<String, String>> buildIncrementalData(int offset, int size) {
    List<Map<String, String>> data = new ArrayList<>();
    for (int i = offset; i < offset+size; i++) {
      Map<String, String> map = new HashMap<>();
      map.put("a", i + "");
      data.add(map);
    }
    return data;
  }

}
