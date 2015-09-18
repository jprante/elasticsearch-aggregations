package org.xbib.elasticsearch.search.aggregations.cardinality;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;

import java.io.IOException;

// TODO

public class SimpleCounters implements Releasable {

    long initialBucketCount;

    public SimpleCounters(long initialBucketCount) {
        this.initialBucketCount = initialBucketCount;
    }

    @Override
    public void close() throws ElasticsearchException {

    }

    public void merge(long thisBucket, SimpleCounters other, long otherBucket) {
    }

    public long maxBucket() {
        return 0L;
    }

    public long cardinality(long bucket) {
        return 0L;
    }

    public void collect(long bucket, long hash) {

    }

    public void writeTo(long bucket, StreamOutput out) throws IOException {
    }

    public static SimpleCounters readFrom(StreamInput in) throws IOException {
        SimpleCounters counters = new SimpleCounters(1);
        return counters;
    }
}
