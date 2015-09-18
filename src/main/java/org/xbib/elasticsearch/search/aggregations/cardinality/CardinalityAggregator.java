package org.xbib.elasticsearch.search.aggregations.cardinality;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.hppc.hash.MurmurHash3;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;

/**
 * An aggregator that computes exact counts of unique values.
 */
public class CardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource valuesSource;
    private final SimpleCounters counts;
    private Collector collector;
    private ValueFormatter formatter;

    public CardinalityAggregator(String name, long estimatedBucketsCount, ValuesSource valuesSource,
                                 @Nullable ValueFormatter formatter,
                                 AggregationContext context, Aggregator parent) {
        super(name, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        this.counts = valuesSource == null ? null : new SimpleCounters(estimatedBucketsCount);
        this.formatter = formatter;
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }


    @Override
    public void setNextReader(AtomicReaderContext reader) {
        postCollectLastCollector();
        collector = createCollector();
    }

    private Collector createCollector() {
        if (valuesSource instanceof ValuesSource.Numeric) {
            ValuesSource.Numeric source = (ValuesSource.Numeric) valuesSource;
            MurmurHash3Values hashValues = source.isFloatingPoint() ? MurmurHash3Values.hash(source.doubleValues()) : MurmurHash3Values.hash(source.longValues());
            return new DirectCollector(counts, hashValues);
        }
        return new DirectCollector(counts, MurmurHash3Values.hash(valuesSource.bytesValues()));
    }


    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        collector.collect(doc, owningBucketOrdinal);
    }

    private void postCollectLastCollector() {
        if (collector != null) {
            try {
                collector.postCollect();
                collector.close();
            } finally {
                collector = null;
            }
        }
    }

    @Override
    protected void doPostCollection() {
        postCollectLastCollector();
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts == null ? 0 : counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (counts == null || owningBucketOrdinal >= counts.maxBucket() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        SimpleCounters copy = new SimpleCounters(1);
        copy.merge(0, counts, owningBucketOrdinal);
        return new InternalCardinality(name, copy, formatter);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinality(name, null, formatter);
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, collector);
    }

    private interface Collector extends Releasable {

        void collect(int doc, long bucketOrd);

        void postCollect();

    }

    private static class EmptyCollector implements Collector {

        @Override
        public void collect(int doc, long bucketOrd) {
            // no-op
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() throws ElasticsearchException {
            // no-op
        }
    }

    private static class DirectCollector implements Collector {

        private final MurmurHash3Values hashes;
        private final SimpleCounters counts;

        DirectCollector(SimpleCounters counts, MurmurHash3Values values) {
            this.counts = counts;
            this.hashes = values;
        }

        @Override
        public void collect(int doc, long bucketOrd) {
            hashes.setDocument(doc);
            final int valueCount = hashes.count();
            for (int i = 0; i < valueCount; ++i) {
                counts.collect(bucketOrd, hashes.valueAt(i));
            }
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() throws ElasticsearchException {
            // no-op
        }

    }

    static abstract class MurmurHash3Values {

        public abstract void setDocument(int docId);

        public abstract int count();

        public abstract long valueAt(int index);

        /**
         * Return a {@link MurmurHash3Values} instance that returns each value as its hash.
         */
        public static MurmurHash3Values cast(final SortedNumericDocValues values) {
            return new MurmurHash3Values() {
                @Override
                public void setDocument(int docId) {
                    values.setDocument(docId);
                }
                @Override
                public int count() {
                    return values.count();
                }
                @Override
                public long valueAt(int index) {
                    return values.valueAt(index);
                }
            };
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each double value.
         */
        public static MurmurHash3Values hash(SortedNumericDoubleValues values) {
            return new Double(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each long value.
         */
        public static MurmurHash3Values hash(SortedNumericDocValues values) {
            return new Long(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each binary value.
         */
        public static MurmurHash3Values hash(SortedBinaryDocValues values) {
            return new Bytes(values);
        }

        private static class Long extends MurmurHash3Values {

            private final SortedNumericDocValues values;

            public Long(SortedNumericDocValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                return MurmurHash3.hash(values.valueAt(index));
            }
        }

        private static class Double extends MurmurHash3Values {

            private final SortedNumericDoubleValues values;

            public Double(SortedNumericDoubleValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                return MurmurHash3.hash(java.lang.Double.doubleToLongBits(values.valueAt(index)));
            }
        }

        private static class Bytes extends MurmurHash3Values {

            private final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();

            private final SortedBinaryDocValues values;

            public Bytes(SortedBinaryDocValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                final BytesRef bytes = values.valueAt(index);
                org.elasticsearch.common.hash.MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
                return hash.h1;
            }
        }
    }
}
