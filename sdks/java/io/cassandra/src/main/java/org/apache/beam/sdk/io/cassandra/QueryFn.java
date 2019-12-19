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
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryFn<T> extends DoFn<Iterable<RingRange>, T> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraIO.class);

  private final CassandraConfig<T> read;

  private transient Cluster cluster;

  private transient Session session;

  private String partitionKey;

  public QueryFn(CassandraConfig<T> read) {
    this.read = read;
  }

  @Setup
  public void setup() {
    this.cluster =
        CassandraIO.getCluster(
            read.hosts(),
            read.port(),
            read.username(),
            read.password(),
            read.localDc(),
            read.consistencyLevel());
    this.session = this.cluster.connect(read.keyspace().get());
    this.partitionKey =
        cluster.getMetadata().getKeyspace(read.keyspace().get()).getTable(read.table().get())
            .getPartitionKey().stream()
            .map(ColumnMetadata::getName)
            .collect(Collectors.joining(","));
  }

  public void teardown() {
    this.session.close();
    this.cluster.close();
  }

  @ProcessElement
  public void processElement(@Element Iterable<RingRange> tokens, OutputReceiver<T> receiver) {

    Mapper<T> mapper = read.mapperFactoryFn().apply(this.session);
    String query = CassandraIO.generateRangeQuery(this.read, partitionKey);
    PreparedStatement preparedStatement = session.prepare(query);

    for (RingRange rr : tokens) {
      Token startToken = cluster.getMetadata().newToken(rr.getStart().toString());
      Token endToken = cluster.getMetadata().newToken(rr.getEnd().toString());
      ResultSet rs =
          session.execute(preparedStatement.bind().setToken(0, startToken).setToken(1, endToken));
      Iterator<T> iter = mapper.map(rs);
      while (iter.hasNext()) {
        receiver.output(iter.next());
      }
    }
  }
}
