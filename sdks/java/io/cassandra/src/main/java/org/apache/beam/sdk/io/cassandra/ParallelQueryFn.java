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
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelQueryFn<T> extends DoFn<Iterable<RingRange>, T> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraIO.class);

  private final Read<T> read;

  transient Cluster cluster;

  transient Session session;

  String partitionKey;

  public ParallelQueryFn(Read<T> read) {
    this.read = read;
  }

  @Setup
  public void setup() {
    this.cluster = CassandraIO.getCluster(
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
    String query = Read.generateRangeQuery(this.read, partitionKey);
    LOG.error("Error with range query, range query is " +  query);
    PreparedStatement preparedStatement = session.prepare(query);

    for (RingRange rr : tokens) {
      Token startToken = cluster.getMetadata().newToken(rr.getStart().toString());
      Token endToken = cluster.getMetadata().newToken(rr.getEnd().toString());
      ResultSet rs = session.execute(preparedStatement.bind().setToken(0, startToken).setToken(1, endToken));
      Iterator<T> iter = mapper.map(rs);
      while(iter.hasNext()) {
        receiver.output(iter.next());
      }
    }
  }


}
