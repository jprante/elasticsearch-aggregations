package org.xbib.elasticsearch.search.aggregations.path;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class PathAggregator extends BucketsAggregator {

    private static final int INITIAL_CAPACITY = 50;
    protected final BytesRefHash bucketOrds;
    private final ValuesSource valuesSource;
    private final BytesRefBuilder previous;
    private final BytesRef separator;
    private final InternalPath.Order order;
    private SortedBinaryDocValues values;


    public PathAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
                          AggregationContext aggregationContext, Aggregator parent, BytesRef separator, InternalPath.Order order) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, INITIAL_CAPACITY, aggregationContext, parent);
        this.valuesSource = valuesSource;
        bucketOrds = new BytesRefHash(estimatedBucketCount, aggregationContext.bigArrays());
        previous = new BytesRefBuilder();
        this.separator = separator;
        this.order = order;
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.bytesValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        values.setDocument(doc);
        final int valuesCount = values.count();
        previous.clear();
        for (int i = 0; i < valuesCount; ++i) {
            final BytesRef bytes = values.valueAt(i);
            if (previous.get().equals(bytes)) {
                continue;
            }
            long bucketOrdinal = bucketOrds.add(bytes);
            if (bucketOrdinal < 0) {
                bucketOrdinal = -1 - bucketOrdinal;
                collectExistingBucket(doc, bucketOrdinal);
            } else {
                collectBucket(doc, bucketOrdinal);
            }
            previous.copyBytes(bytes);
        }
    }

    @Override
    public InternalPath buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;
        List<Path.Bucket> buckets = new ArrayList<>();
        InternalPath.Bucket spare;
        for (long i = 0; i < bucketOrds.size(); i++) {
            spare = new InternalPath.Bucket(null, new BytesRef(), 0, null, 0, null);
            BytesRef term = new BytesRef();
            bucketOrds.get(i, term);
            String[] paths = term.utf8ToString().split(Pattern.quote(separator.utf8ToString()));
            spare.termBytes = BytesRef.deepCopyOf(term);
            spare.docCount = bucketDocCount(i);
            spare.aggregations = bucketAggregations(i);
            spare.level = paths.length - 1;
            spare.val = paths[paths.length - 1];
            spare.path = Arrays.copyOf(paths, paths.length - 1);
            buckets.add(spare);
        }
        return new InternalPath(name, buckets, order, separator);
    }

    @Override
    public InternalPath buildEmptyAggregation() {
        return new InternalPath(name, new ArrayList<Path.Bucket>(), order, separator);
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

}