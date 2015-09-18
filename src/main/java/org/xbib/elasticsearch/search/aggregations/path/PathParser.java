package org.xbib.elasticsearch.search.aggregations.path;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;

public class PathParser implements Aggregator.Parser {

    public static final String DEFAULT_SEPARATOR = "/";
    public static final int DEFAULT_MIN_DEPTH = 0;
    public static final int DEFAULT_MAX_DEPTH = 2;

    private static InternalPath.Order resolveOrder(String key, boolean asc) {
        if ("_term".equals(key)) {
            return (InternalPath.Order) (asc ? InternalPath.Order.KEY_ASC : InternalPath.Order.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalPath.Order) (asc ? InternalPath.Order.COUNT_ASC : InternalPath.Order.COUNT_DESC);
        }
        return new InternalPath.Aggregation(key, asc);
    }

    @Override
    public String type() {
        return InternalPath.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
        ValuesSourceParser vsParser = ValuesSourceParser.any(aggregationName, InternalPath.TYPE, context).scriptable(true).build();
        String separator = DEFAULT_SEPARATOR;
        int maxDepth = DEFAULT_MAX_DEPTH;
        int minDepth = DEFAULT_MIN_DEPTH;
        boolean depth = false;
        InternalPath.Order order = (InternalPath.Order) Path.Order.KEY_ASC;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("separator".equals(currentFieldName)) {
                    separator = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (!depth && "max_depth".equals(currentFieldName)) {
                    maxDepth = parser.intValue();
                } else if (!depth && "min_depth".equals(currentFieldName)) {
                    minDepth = parser.intValue();
                } else if ("depth".equals(currentFieldName)) {
                    minDepth = maxDepth = parser.intValue();
                    depth = true;
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("order".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            order = resolveOrder(currentFieldName, "asc".equals(parser.text()));
                        }
                    }
                }
            }
        }
        if (minDepth > maxDepth) {
            throw new SearchParseException(context, "min_depth paramater must be lower than max_depth parameter");
        }
        return new PathFactory(aggregationName, vsParser.config(), new BytesRef(separator), minDepth, maxDepth, order);
    }

    private static class PathFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

        private final BytesRef separator;
        private final int maxDepth;
        private final int minDepth;
        private final InternalPath.Order order;

        public PathFactory(String name, ValuesSourceConfig<ValuesSource> config, BytesRef separator,
                           int minDepth, int maxDepth, InternalPath.Order order) {
            super(name, InternalPath.TYPE.name(), config);
            this.separator = separator;
            this.minDepth = minDepth;
            this.maxDepth = maxDepth;
            this.order = order;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            final InternalAggregation aggregation = new InternalPath(name, new ArrayList<Path.Bucket>(), order, separator);
            return new NonCollectingAggregator(name, aggregationContext, parent) {
                public InternalAggregation buildEmptyAggregation() {
                    return aggregation;
                }
            };
        }

        @Override
        protected Aggregator create(final ValuesSource valuesSource, long expectedBucketsCount,
                                    AggregationContext aggregationContext, Aggregator parent) {
            final SortingValues sortingValues = new SortingValues(valuesSource, separator, minDepth, maxDepth);
            ValuesSource.Bytes valuesSourceBytes = new PathSource(sortingValues, valuesSource.metaData());
            return new PathAggregator(name, factories, valuesSourceBytes, aggregationContext, parent, separator, order);
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
                SortedBinaryDocValues sortedBinaryDocValues = valuesSource.bytesValues();
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
            }
        }

        private static class PathSource extends ValuesSource.Bytes {

            private final SortedBinaryDocValues values;
            private final MetaData metaData;

            public PathSource(SortedBinaryDocValues values, MetaData delegates) {
                this.values = values;
                this.metaData = MetaData.builder(delegates).uniqueness(MetaData.Uniqueness.UNKNOWN).build();
            }

            @Override
            public SortedBinaryDocValues bytesValues() {
                return values;
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }
        }
    }

}