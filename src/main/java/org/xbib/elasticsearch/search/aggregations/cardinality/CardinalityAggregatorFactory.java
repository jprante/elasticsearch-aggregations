package org.xbib.elasticsearch.search.aggregations.cardinality;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

final class CardinalityAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

    CardinalityAggregatorFactory(String name, ValuesSourceConfig config) {
        super(name, InternalCardinality.TYPE.name(), config);
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext context, Aggregator parent) {
        return new CardinalityAggregator(name,
                parent == null ? 1 : parent.estimatedBucketCount(),
                null, config.formatter(), context, parent);
    }

    @Override
    protected Aggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext context, Aggregator parent) {
        return new CardinalityAggregator(name,
                parent == null ? 1 : parent.estimatedBucketCount(),
                valuesSource, config.formatter(), context, parent);
    }

}
