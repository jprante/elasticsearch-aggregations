package org.xbib.elasticsearch.search.aggregations.cardinality;

import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

public interface Cardinality extends NumericMetricsAggregation.SingleValue {

    long getValue();

}
