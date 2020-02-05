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
package org.apache.beam.sdk.io.cassandra;

import static junit.framework.TestCase.assertTrue;
import static org.apache.beam.sdk.io.cassandra.CassandraIO.Read.distance;
import static org.apache.beam.sdk.io.cassandra.CassandraIO.isMurmur3Partitioner;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Computed;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.embedded.CassandraShutDownHook;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests of {@link CassandraIO}. */
@RunWith(JUnit4.class)
public class CassandraIOTest implements Serializable {
  private static final long NUM_ROWS = 20L;
  private static final String CASSANDRA_KEYSPACE = "beam_ks";
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final String CASSANDRA_TABLE = "scientist";
  private static final Logger LOG = LoggerFactory.getLogger(CassandraIOTest.class);
  private static final String STORAGE_SERVICE_MBEAN = "org.apache.cassandra.db:type=StorageService";
  private static final float ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE = 0.5f;
  private static final int FLUSH_TIMEOUT = 30000;
  private static final int JMX_CONF_TIMEOUT = 1000;
  private static int jmxPort;
  private static int cassandraPort;

  private static Cluster cluster;
  private static Session session;

  private static final String TEMPORARY_FOLDER =
      System.getProperty("java.io.tmpdir") + "/embedded-cassandra/";
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  private static CassandraShutDownHook shutdownHook;

  @BeforeClass
  public static void beforeClass() throws Exception {
    jmxPort = NetworkTestHelper.getAvailableLocalPort();
    shutdownHook = new CassandraShutDownHook();
    String data = TEMPORARY_FOLDER + "/data";
    Files.createDirectories(Paths.get(data));
    String commitLog = TEMPORARY_FOLDER + "/commit-log";
    Files.createDirectories(Paths.get(commitLog));
    String cdcRaw = TEMPORARY_FOLDER + "/cdc-raw";
    Files.createDirectories(Paths.get(cdcRaw));
    String hints = TEMPORARY_FOLDER + "/hints";
    Files.createDirectories(Paths.get(hints));
    String savedCache = TEMPORARY_FOLDER + "/saved-cache";
    Files.createDirectories(Paths.get(savedCache));
    CassandraEmbeddedServerBuilder builder =
        CassandraEmbeddedServerBuilder.builder()
            .withKeyspaceName(CASSANDRA_KEYSPACE)
            .withDataFolder(data)
            .withCommitLogFolder(commitLog)
            .withCdcRawFolder(cdcRaw)
            .withHintsFolder(hints)
            .withSavedCachesFolder(savedCache)
            .withShutdownHook(shutdownHook)
            // randomized CQL port at startup
            .withJMXPort(jmxPort)
            .cleanDataFilesAtStartup(false);

    // under load we get a NoHostAvailable exception at cluster creation,
    // so retry to create it every 1 sec up to 3 times.
    cluster = buildCluster(builder);

    cassandraPort = cluster.getConfiguration().getProtocolOptions().getPort();
    session = CassandraIOTest.cluster.newSession();
    insertData();
    disableAutoCompaction();
  }

  private static Cluster buildCluster(CassandraEmbeddedServerBuilder builder) {
    int tried = 0;
    while (tried < 3) {
      try {
        return builder.buildNativeCluster();
      } catch (NoHostAvailableException e) {
        tried++;
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e1) {
        }
      }
    }
    throw new RuntimeException("Unable to create embedded Cassandra cluster");
  }

  @AfterClass
  public static void afterClass() throws InterruptedException, IOException {
    shutdownHook.shutDownNow();
    FileUtils.deleteDirectory(new File(TEMPORARY_FOLDER));
  }

  private static void insertData() throws Exception {
    LOG.info("Create Cassandra tables");
    session.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s(person_department text, person_id int, person_name text, PRIMARY KEY"
                + "((person_department), person_id));",
            CASSANDRA_KEYSPACE, CASSANDRA_TABLE));
    session.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s(person_department text, person_id int, person_name text, PRIMARY KEY"
                + "((person_department), person_id));",
            CASSANDRA_KEYSPACE, CASSANDRA_TABLE_WRITE));


    LOG.info("Insert records");
    String[][] scientists = {
      new String[] {"phys", "Einstein"},
      new String[] {"bio", "Darwin"},
      new String[] {"phys", "Copernicus"},
      new String[] {"bio", "Pasteur"},
      new String[] {"bio", "Curie"},
      new String[] {"phys", "Faraday"},
      new String[] {"math", "Newton"},
      new String[] {"phys", "Bohr"},
      new String[] {"phys", "Galilei"},
      new String[] {"math", "Maxwell"},
    };
    for (int i = 0; i < NUM_ROWS; i++) {
      int index = i % scientists.length;
      String insertStr =
          String.format(
              "INSERT INTO %s.%s(person_department, person_id, person_name) values("
                  + "'"
                  + scientists[index][0]
                  + "', "
                  + i
                  + ", '"
                  + scientists[index][1]
                  + "');",
              CASSANDRA_KEYSPACE,
              CASSANDRA_TABLE);
      LOG.error("Error with str = " + insertStr);
      session.execute(insertStr);
    }
    flushMemTables();
  }

  /**
   * Force the flush of cassandra memTables to SSTables to update size_estimates.
   * https://wiki.apache.org/cassandra/MemtableSSTable This is what cassandra spark connector does
   * through nodetool binary call. See:
   * https://github.com/datastax/spark-cassandra-connector/blob/master/spark-cassandra-connector
   * /src/it/scala/com/datastax/spark/connector/rdd/partitioner/DataSizeEstimatesSpec.scala which
   * uses the same JMX service as bellow. See:
   * https://github.com/apache/cassandra/blob/cassandra-3.X
   * /src/java/org/apache/cassandra/tools/nodetool/Flush.java
   */
  @SuppressWarnings("unused")
  private static void flushMemTables() throws Exception {
    JMXServiceURL url =
        new JMXServiceURL(
            String.format(
                "service:jmx:rmi://%s/jndi/rmi://%s:%s/jmxrmi",
                CASSANDRA_HOST, CASSANDRA_HOST, jmxPort));
    JMXConnector jmxConnector = JMXConnectorFactory.connect(url, null);
    MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
    ObjectName objectName = new ObjectName(STORAGE_SERVICE_MBEAN);
    StorageServiceMBean mBeanProxy =
        JMX.newMBeanProxy(mBeanServerConnection, objectName, StorageServiceMBean.class);
    mBeanProxy.forceKeyspaceFlush(CASSANDRA_KEYSPACE, CASSANDRA_TABLE);
    jmxConnector.close();
    Thread.sleep(FLUSH_TIMEOUT);
  }

  /**
   * Disable auto compaction on embedded cassandra host, to avoid race condition in temporary files
   * cleaning.
   */
  @SuppressWarnings("unused")
  private static void disableAutoCompaction() throws Exception {
    JMXServiceURL url =
        new JMXServiceURL(
            String.format(
                "service:jmx:rmi://%s/jndi/rmi://%s:%s/jmxrmi",
                CASSANDRA_HOST, CASSANDRA_HOST, jmxPort));
    JMXConnector jmxConnector = JMXConnectorFactory.connect(url, null);
    MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
    ObjectName objectName = new ObjectName(STORAGE_SERVICE_MBEAN);
    StorageServiceMBean mBeanProxy =
        JMX.newMBeanProxy(mBeanServerConnection, objectName, StorageServiceMBean.class);
    mBeanProxy.disableAutoCompaction(CASSANDRA_KEYSPACE, CASSANDRA_TABLE);
    jmxConnector.close();
    Thread.sleep(JMX_CONF_TIMEOUT);
  }

  @Test
  public void testRead() throws Exception {
    PCollection<Scientist> output =
        pipeline.apply(
            CassandraIO.<Scientist>read()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withTable(CASSANDRA_TABLE)
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(NUM_ROWS);

    PCollection<KV<String, Integer>> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<Scientist, KV<String, Integer>>() {
                  @Override
                  public KV<String, Integer> apply(Scientist scientist) {
                    return KV.of(scientist.name, scientist.id);
                  }
                }));
    PAssert.that(mapped.apply("Count occurrences per scientist", Count.perKey()))
        .satisfies(
            input -> {
              for (KV<String, Long> element : input) {
                assertEquals(element.getKey(), NUM_ROWS / 10, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testReadAll() {
    RingRange bioRR =
        RingRange.fromEncodedKey(
            cluster.getMetadata(), TypeCodec.varchar().serialize("bio", ProtocolVersion.V3));

    RingRange mathRR =
        RingRange.fromEncodedKey(
            cluster.getMetadata(), TypeCodec.varchar().serialize("math", ProtocolVersion.V3));

    PCollection<Scientist> output =
        pipeline
            .apply(
                Create.of(
                    CassandraIO.<Scientist>read().withRingRange(bioRR),
                    CassandraIO.<Scientist>read().withRingRange(mathRR)))
            .apply(
                CassandraIO.<Scientist>readAll()
                    .withSplitCount(2)
                    .withHosts(Collections.singletonList(CASSANDRA_HOST))
                    .withPort(cassandraPort)
                    .withKeyspace(CASSANDRA_KEYSPACE)
                    .withTable(CASSANDRA_TABLE)
                    .withCoder(SerializableCoder.of(Scientist.class))
                    .withEntity(Scientist.class));

    PCollection<KV<String, Integer>> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<Scientist, KV<String, Integer>>() {
                  @Override
                  public KV<String, Integer> apply(Scientist scientist) {
                    return KV.of(scientist.department, scientist.id);
                  }
                }));
    PAssert.that(mapped.apply("Count occurrences per scientist", Count.perKey()))
        .satisfies(
            input -> {
              HashMap<String, Long> map = new HashMap<>();
              for (KV<String, Long> element : input) {
                map.put(element.getKey(), element.getValue());
              }
              assertTrue(map.size() == 2);
              assertEquals(map.get("bio"), 6L, 0L);
              assertEquals(map.get("math"), 4L, 0L);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testReadWithQuery() throws Exception {
    PCollection<Scientist> output =
        pipeline.apply(
            CassandraIO.<Scientist>read()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withTable(CASSANDRA_TABLE)
                .withQuery(
                    "select person_id, writetime(person_name) from beam_ks.scientist where person_id=10 AND person_department='phys'")
                .withCoder(SerializableCoder.of(Scientist.class))
                .withEntity(Scientist.class));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(1L);
    PAssert.that(output)
        .satisfies(
            input -> {
              for (Scientist sci : input) {
                assertNull(sci.name);
                assertTrue(sci.nameTs != null && sci.nameTs > 0);
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testWrite() {
    ArrayList<ScientistWrite> data = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      ScientistWrite scientist = new ScientistWrite();
      scientist.id = i;
      scientist.name = "Name " + i;
      scientist.department = "bio";
      data.add(scientist);
    }

    pipeline
        .apply(Create.of(data))
        .apply(
            CassandraIO.<ScientistWrite>write()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withEntity(ScientistWrite.class));
    // table to write to is specified in the entity in @Table annotation (in that case scientist)
    pipeline.run();

    List<Row> results = getRows(CASSANDRA_TABLE_WRITE);
    assertEquals(NUM_ROWS, results.size());
    for (Row row : results) {
      assertTrue(row.getString("person_name").matches("Name (\\d*)"));
    }
  }

  private static final AtomicInteger counter = new AtomicInteger();

  private static class NOOPMapperFactory implements SerializableFunction<Session, Mapper> {

    @Override
    public Mapper apply(Session input) {
      return new NOOPMapper();
    }
  }

  private static class NOOPMapper implements Mapper<String>, Serializable {

    private final ListeningExecutorService executor =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));

    final Callable<Void> asyncTask = () -> (null);

    @Override
    public Iterator map(ResultSet resultSet) {
      if (!resultSet.isExhausted()) {
        resultSet.iterator().forEachRemaining(r -> counter.getAndIncrement());
      }
      return Collections.emptyIterator();
    }

    @Override
    public Future<Void> deleteAsync(String entity) {
      counter.incrementAndGet();
      return executor.submit(asyncTask);
    }

    @Override
    public Future<Void> saveAsync(String entity) {
      counter.incrementAndGet();
      return executor.submit(asyncTask);
    }
  }

  @Test
  public void testReadWithMapper() throws Exception {
    counter.set(0);

    SerializableFunction<Session, Mapper> factory = new NOOPMapperFactory();

    pipeline.apply(
        CassandraIO.<String>read()
            .withHosts(Collections.singletonList(CASSANDRA_HOST))
            .withPort(cassandraPort)
            .withKeyspace(CASSANDRA_KEYSPACE)
            .withTable(CASSANDRA_TABLE)
            .withCoder(SerializableCoder.of(String.class))
            .withEntity(String.class)
            .withMapperFactoryFn(factory));
    pipeline.run();

    assertEquals(NUM_ROWS, counter.intValue());
  }

  @Test
  public void testCustomMapperImplWrite() throws Exception {
    counter.set(0);

    SerializableFunction<Session, Mapper> factory = new NOOPMapperFactory();

    pipeline
        .apply(Create.of(""))
        .apply(
            CassandraIO.<String>write()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withMapperFactoryFn(factory)
                .withEntity(String.class));
    pipeline.run();

    assertEquals(1, counter.intValue());
  }

  @Test
  public void testCustomMapperImplDelete() {
    counter.set(0);

    SerializableFunction<Session, Mapper> factory = new NOOPMapperFactory();

    pipeline
        .apply(Create.of(""))
        .apply(
            CassandraIO.<String>delete()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withMapperFactoryFn(factory)
                .withEntity(String.class));
    pipeline.run();

    assertEquals(1, counter.intValue());
  }

  private List<Row> getRows(String table) {
    ResultSet result =
        session.execute(
            String.format("select person_id,person_name from %s.%s", CASSANDRA_KEYSPACE, table));
    return result.all();
  }

  // TEMP TEST
  @Test
  public void testDelete() throws Exception {
    List<Row> results = getRows(CASSANDRA_TABLE);
    assertEquals(NUM_ROWS, results.size());

    Scientist einstein = new Scientist();
    einstein.id = 0;
    einstein.department = "phys";
    einstein.name = "Einstein";
    pipeline
        .apply(Create.of(einstein))
        .apply(
            CassandraIO.<Scientist>delete()
                .withHosts(Collections.singletonList(CASSANDRA_HOST))
                .withPort(cassandraPort)
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withEntity(Scientist.class));

    pipeline.run();
    results = getRows(CASSANDRA_TABLE);
    assertEquals(NUM_ROWS - 1, results.size());
    // re-insert suppressed doc to make the test autonomous
    session.execute(
        String.format(
            "INSERT INTO %s.%s(person_department, person_id, person_name) values("
                + "'phys', "
                + einstein.id
                + ", '"
                + einstein.name
                + "');",
            CASSANDRA_KEYSPACE,
            CASSANDRA_TABLE));
  }

  @Test
  public void testValidPartitioner() {
    Assert.assertTrue(isMurmur3Partitioner(cluster));
  }

  @Test
  public void testDistance() {
    BigInteger dist = distance(new BigInteger("10"), new BigInteger("100"));
    assertEquals(BigInteger.valueOf(90), dist);

    dist = distance(new BigInteger("100"), new BigInteger("10"));
    assertEquals(new BigInteger("18446744073709551526"), dist);
  }

  /** Simple Cassandra entity used in read tests. */
  @Table(name = CASSANDRA_TABLE, keyspace = CASSANDRA_KEYSPACE)
  static class Scientist implements Serializable {

    @Column(name = "person_name")
    String name;

    @Computed("writetime(person_name)")
    Long nameTs;

    @ClusteringColumn()
    @Column(name = "person_id")
    int id;

    @PartitionKey
    @Column(name = "person_department")
    String department;

    @Override
    public String toString() {
      return id + ":" + name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Scientist scientist = (Scientist) o;
      return id == scientist.id
          && Objects.equal(name, scientist.name)
          && Objects.equal(department, scientist.department);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, id);
    }
  }

  private static final String CASSANDRA_TABLE_WRITE = "scientist_write";
  /** Simple Cassandra entity used in write tests. */
  @Table(name = CASSANDRA_TABLE_WRITE, keyspace = CASSANDRA_KEYSPACE)
  static class ScientistWrite extends Scientist {}
}
