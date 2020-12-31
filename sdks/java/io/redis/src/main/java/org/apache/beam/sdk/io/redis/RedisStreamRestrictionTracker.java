package org.apache.beam.sdk.io.redis;

import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import java.io.Serializable;
import org.apache.beam.sdk.io.redis.StreamDescriptor.StreamConsumer;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RedisStreamRestrictionTracker extends
        RestrictionTracker<StreamRestriction, StreamEntryID> implements Serializable {
  
    private static final long serialVersionUID = 1L;

    StreamEntryID current = new StreamEntryID();

    StreamConsumer streamConsumer;

    public RedisStreamRestrictionTracker(StreamRestriction restriction) {          
      this.streamConsumer = restriction.getConsumer();
      this.current = restriction.getCurrentEntry();
    }

    @Override
    public boolean tryClaim(StreamEntryID position) {
      this.current = position;
      return true;
    }

    @Override
    public StreamRestriction currentRestriction() {
      return new StreamRestriction(this.streamConsumer, this.current); 
    }

    @Override
    public @Nullable SplitResult<StreamRestriction> trySplit(double fractionOfRemainder) {
      StreamRestriction current = new StreamRestriction(this.streamConsumer, this.current);
      StreamConsumer newConsumer = new StreamConsumer(this.streamConsumer.getName(), this.streamConsumer.getCount()+1);
      //new consumer has no need to go through pending results, so we start at >
      StreamRestriction newRestriction = new StreamRestriction(newConsumer, StreamEntryID.UNRECEIVED_ENTRY);
      return SplitResult.of(current, newRestriction);
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      return IsBounded.UNBOUNDED;
    }
}

