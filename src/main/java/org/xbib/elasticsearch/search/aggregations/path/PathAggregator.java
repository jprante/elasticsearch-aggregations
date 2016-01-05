package org.xbib.elasticsearch.search.aggregations.path;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class PathAggregator extends BucketsAggregator {

    private final static ESLogger logger = ESLoggerFactory.getLogger(PathAggregator.class.getName());

    private final ValuesSource valuesSource;
    private final BytesRefHash bucketOrds;
    private final BytesRef separator;
    private final Path.Order order;

    public PathAggregator(String name,
                          AggregatorFactories factories,
                          ValuesSource valuesSource,
                          AggregationContext context,
                          Aggregator parent,
                          List<PipelineAggregator> pipelineAggregators,
                          Map<String, Object> metaData,
                          BytesRef separator,
                          Path.Order order) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.bucketOrds = new BytesRefHash(1, context.bigArrays());
        this.separator = separator;
        this.order = order;
        logger.info("new: {}", name);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
//                logger.info("collect: doc={} bucket={} count={}", doc, bucket, values.count());
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
                        bucketOrdinal = - 1 - bucketOrdinal;
                        collectExistingBucket(sub, doc, bucketOrdinal);
                    } else {
                        collectBucket(sub, doc, bucketOrdinal);
                    }
                    previous.copyBytes(bytes);
                }
            }
        };
    }

    @Override
    public InternalPath buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        List<InternalPath.Bucket> buckets = new ArrayList<>();
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
//        logger.info("buildAggregation {}", owningBucketOrdinal);
        return new InternalPath(name, pipelineAggregators(), metaData(), buckets, order, separator);
    }

    @Override
    public InternalPath buildEmptyAggregation() {
        return new InternalPath(name, pipelineAggregators(), metaData(), new ArrayList<>(), order, separator);
    }

    @Override
    public void doClose() {
        super.doClose();
        Releasables.close(bucketOrds);
    }

}