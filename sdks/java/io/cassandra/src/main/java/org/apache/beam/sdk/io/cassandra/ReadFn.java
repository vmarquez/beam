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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import java.util.Iterator;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.cassandra.CassandraIO.Read;
import org.apache.beam.sdk.io.cassandra.CassandraIO.ReadAll;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReadFn<T> extends DoFn<Read<T>, T> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadFn.class);

  private transient Cluster cluster;

  private transient Session session;

  private final ReadAll<T> readAll;

  public ReadFn(ReadAll<T> readAll) {
    this.readAll = readAll;
  }

  @Setup
  public void setup() {
    this.cluster =
        CassandraIO.getCluster(
            readAll.hosts(),
            readAll.port(),
            readAll.username(),
            readAll.password(),
            readAll.localDc(),
            readAll.consistencyLevel());
    this.session = cluster.connect(readAll.keyspace().get());
  }

  @Teardown
  public void teardown() {
    this.session.close();
    this.cluster.close();
  }

  @ProcessElement
  public void processElement(@Element Read<T> read, OutputReceiver<T> receiver) {
    Mapper<T> mapper = read.mapperFactoryFn().apply(session);
    String partitionKey =
        cluster.getMetadata().getKeyspace(read.keyspace().get()).getTable(read.table().get())
            .getPartitionKey().stream()
            .map(ColumnMetadata::getName)
            .collect(Collectors.joining(","));
    String query = generateRangeQuery(read, partitionKey);
    PreparedStatement preparedStatement = session.prepare(query);
    RingRange rr = read.ringRange().get();
    Token startToken = cluster.getMetadata().newToken(rr.getStart().toString());
    Token endToken = cluster.getMetadata().newToken(rr.getEnd().toString());
    ResultSet rs =
        session.execute(preparedStatement.bind().setToken(0, startToken).setToken(1, endToken));
    Iterator<T> iter = mapper.map(rs);
    while (iter.hasNext()) {
      receiver.output(iter.next());
    }
  }

  private static String generateRangeQuery(Read<?> spec, String partitionKey) {
    final String rangeFilter =
        Joiner.on(" AND ")
            .skipNulls()
            .join(
                String.format("(token(%s) >= ?)", partitionKey),
                String.format("(token(%s) < ?)", partitionKey));
    final String query =
        (spec.query() == null)
            ? buildQuery(spec) + " WHERE " + rangeFilter
            : buildQuery(spec) + " AND " + rangeFilter;
    LOG.debug("CassandraIO generated query : {}", query);
    return query;
  }

  private static String buildQuery(Read<?> spec) {
    return (spec.query() == null)
        ? String.format("SELECT * FROM %s.%s", spec.keyspace().get(), spec.table().get())
        : spec.query().get();
  }
}
