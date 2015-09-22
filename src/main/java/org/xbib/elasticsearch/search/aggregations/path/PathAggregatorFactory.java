package org.xbib.elasticsearch.search.aggregations.path;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PathAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(PathAggregatorFactory.class.getName());

    private final BytesRef separator;
    private final int maxDepth;
    private final int minDepth;
    private final Path.Order order;

    public PathAggregatorFactory(String name,
                                 ValuesSourceConfig<ValuesSource> config,
                                 BytesRef separator,
                                 int minDepth,
                                 int maxDepth,
                                 Path.Order order) {
        super(name, InternalPath.TYPE.name(), config);
        this.separator = separator;
        this.minDepth = minDepth;
        this.maxDepth = maxDepth;
        this.order = order;
        logger.info("new name={} sep={}", name, separator);
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext,
                                        Aggregator parent,
                                        List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData) throws IOException {
        final InternalAggregation aggregation = new InternalPath(name, pipelineAggregators, metaData, new ArrayList<>(), order, separator);
        return new NonCollectingAggregator(name, aggregationContext, parent, factories, pipelineAggregators, metaData) {
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent,
                                          boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        logger.info("doCreateInternal value source={} collectsFromSingleBucket={}",
                valuesSource.toString(), collectsFromSingleBucket);
        if (!collectsFromSingleBucket) {
            return asMultiBucketAggregator(this, aggregationContext, parent);
        }
        final SortingValues sortingValues = new SortingValues(valuesSource, separator, minDepth, maxDepth);
        ValuesSource.Bytes valuesSourceBytes = new PathSource(sortingValues);
        return new PathAggregator(name,
                factories,
                valuesSourceBytes,
                aggregationContext,
                parent,
                pipelineAggregators,
                metaData,
                separator,
                order);
    }

    private static class SortingValues extends SortingBinaryDocValues {

        private final ValuesSource valuesSource;
        private final BytesRef separator;
        private final int minDepth;
        private final int maxDepth;

        protected SortingValues(ValuesSource valuesSource, BytesRef separator, int minDepth, int maxDepth) {
            this.valuesSource = valuesSource;
            this.separator = separator;
            this.minDepth = minDepth;
            this.maxDepth = maxDepth;
        }

        @Override
        public void setDocument(int docId) {
            try {
                SortedBinaryDocValues sortedBinaryDocValues = valuesSource.bytesValues(null);
                sortedBinaryDocValues.setDocument(docId);
                count = sortedBinaryDocValues.count();
                grow();
                int t = 0;
                for (int i = 0; i < sortedBinaryDocValues.count(); i++) {
                    int depth = 0;
                    int lastOff = 0;
                    BytesRef val = sortedBinaryDocValues.valueAt(i);
                    BytesRefBuilder cleanVal = new BytesRefBuilder();
                    for (int off = 0; off < val.length; off++) {
                        if (new BytesRef(val.bytes, val.offset + off, separator.length).equals(separator)) {
                            if (off - lastOff > 1) {
                                if (cleanVal.length() > 0) {
                                    cleanVal.append(separator);
                                }
                                if (minDepth > depth) {
                                    depth++;
                                    off += separator.length - 1;
                                    lastOff = off + 1;
                                    continue;
                                }
                                cleanVal.append(val.bytes, val.offset + lastOff, off - lastOff);
                                values[t++].copyBytes(cleanVal);
                                depth++;
                                if (maxDepth >= 0 && depth > maxDepth) {
                                    break;
                                }
                                off += separator.length - 1;
                                lastOff = off + 1;
                                count++;
                                grow();
                            } else {
                                lastOff = off + separator.length;
                            }
                        } else if (off == val.length - 1) {
                            if (cleanVal.length() > 0) {
                                cleanVal.append(separator);
                            }
                            if (depth >= minDepth) {
                                cleanVal.append(val.bytes, val.offset + lastOff, off - lastOff + 1);
                            }
                        }
                    }
                    if (maxDepth >= 0 && depth > maxDepth) {
                        continue;
                    }
                    values[t++].copyBytes(cleanVal);
                }
                sort();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class PathSource extends ValuesSource.Bytes {

        private final SortedBinaryDocValues values;

        public PathSource(SortedBinaryDocValues values) {
            this.values = values;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
            return values;
        }

    }
}
