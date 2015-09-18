package org.xbib.elasticsearch.search.aggregations.cardinality;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

import java.io.IOException;

public class CardinalityBuilder extends ValuesSourceMetricsAggregationBuilder<CardinalityBuilder> {

    public CardinalityBuilder(String name) {
        super(name, InternalCardinality.TYPE.name());
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        super.internalXContent(builder, params);
    }

}
