package org.xbib.elasticsearch.plugin.aggregations;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.aggregations.AggregationModule;
import org.xbib.elasticsearch.search.aggregations.cardinality.CardinalityParser;
import org.xbib.elasticsearch.search.aggregations.cardinality.InternalCardinality;
import org.xbib.elasticsearch.search.aggregations.path.InternalPath;
import org.xbib.elasticsearch.search.aggregations.path.PathParser;

public class AggregationPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "aggregations";
    }

    @Override
    public String description() {
        return "More aggregations";
    }

    public void onModule(AggregationModule aggModule) {
        aggModule.addAggregatorParser(CardinalityParser.class);
        InternalCardinality.registerStreams();
        aggModule.addAggregatorParser(PathParser.class);
        InternalPath.registerStreams();
    }
}

