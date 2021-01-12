/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.redis;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.redis.RedisStreamIO.ReadStream;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
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
public class RedisIOSTreamTest {
  private static final String REDIS_HOST = "localhost";

  private static final String REDIS_KEY = "key_" + UUID.randomUUID().toString().substring(0, 7);

  private static final String REDIS_GROUPNAME =
      "group_" + UUID.randomUUID().toString().substring(0, 7);

  private static final String REDIS_CONSUMERPREFIX =
      " consumer_" + UUID.randomUUID().toString().substring(0, 7);

  @Rule public TestPipeline p = TestPipeline.create();

  private static RedisServer server;
  private static int port = 6379;

  private static Jedis jedis;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // port = NetworkTestHelper.getAvailableLocalPort();
    // server = new RedisServer(port);
    // server.start();
    jedis = RedisConnectionConfiguration.create(REDIS_HOST, port).connect();
  }

  @AfterClass
  public static void afterClass() {
    jedis.close();
    // server.stop();
  }

  // @Test
  public void XtestReadStream() {
    List<Map<String, String>> data = buildIncrementalData(0, 10);
    data.forEach(map -> jedis.xadd(REDIS_KEY, StreamEntryID.NEW_ENTRY, map));
    PCollection<Map<String, String>> pcoll =
        p.apply("read", getReadStream(REDIS_KEY, 10, REDIS_GROUPNAME))
            .apply("map to map", MapElements.into(mapDescriptor()).via(se -> se.getFields()));

    PAssert.that(pcoll).containsInAnyOrder(data);
    PipelineResult result = p.run();
    result.waitUntilFinish(Duration.standardSeconds(3));

    // List<StreamPendingEntry> pending = jedis.xpending(newKey, groupName, new StreamEntryID(),
    // StreamEntryID.LAST_ENTRY, 10, "consumername");
  }

  @Test
  public void testReadAfterCrash() throws InterruptedException, IOException {
    String newKey = REDIS_KEY + "crashTest";
    String groupName = REDIS_GROUPNAME + "testCrash";

    System.out.println("key = " + newKey + " groupName = " + groupName);
    List<Map<String, String>> data = buildIncrementalData(0, 10);

    // data1.stream().forEach(map -> jedis.xadd(newKey, StreamEntryID.NEW_ENTRY, map));
    data.stream().forEach(map -> jedis.xadd(newKey, StreamEntryID.NEW_ENTRY, map));
    System.out.println("creating");
    jedis.xgroupCreate(newKey, groupName, new StreamEntryID(), true);
    // loop to pull 5 in pending
    for (int x = 0; x < 5; x++) {
      Map.Entry<String, StreamEntryID> mapEntry =
          new AbstractMap.SimpleImmutableEntry<>(newKey, StreamEntryID.UNRECEIVED_ENTRY);
      List<Map.Entry<String, List<StreamEntry>>> list =
          Optional.of(
                  jedis.xreadGroup(groupName, REDIS_CONSUMERPREFIX + "1", 1, 1000, false, mapEntry))
              .orElse(new ArrayList<>());
    }

    PCollection<String> pcoll =
        p.apply("read", getReadStream(newKey, 10, groupName))
            .apply("Map", MapElements.into(TypeDescriptors.strings()).via(se -> fieldToString(se)));

    PAssert.that(pcoll)
        .containsInAnyOrder(data.stream().map(m -> "a," + m.get("a")).collect(Collectors.toList()));
    PipelineResult rr = p.run();

    //   rr.waitUntilFinish(org.joda.time.Duration.standardSeconds(4));
  }

  public ReadStream getReadStream(String key, long maxNumRecords, String groupName) {
    return RedisStreamIO.read()
        .withEndpoint(REDIS_HOST, port)
        .withKeyPattern(key)
        .withBatchSize(3)
        .withGroupId(groupName)
        .withTimeout(1000)
        .withConsumerPrefix(REDIS_CONSUMERPREFIX)
        .withMaxNumRecords(maxNumRecords);
  }

  public static String fieldToString(StreamEntry se) {
    String str = se.getFields().values().stream().collect(Collectors.joining(","));
    return "a," + str;
  }

  public String mapToString(Map<String, String> map) {
    return map.values().stream().collect(Collectors.joining(","));
  }

  public TypeDescriptor<Map<String, String>> mapDescriptor() {
    return TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.strings());
  }

  private List<Map<String, String>> buildIncrementalData(int offset, int size) {
    List<Map<String, String>> data = new ArrayList<>();
    for (int i = offset; i < offset + size; i++) {
      Map<String, String> map = new HashMap<>();
      map.put("a", i + "");
      data.add(map);
    }
    return data;
  }
}
