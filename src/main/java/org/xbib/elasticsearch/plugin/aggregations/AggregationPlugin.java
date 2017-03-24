package org.xbib.elasticsearch.plugin.aggregations;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchModule;
import org.xbib.elasticsearch.search.aggregations.path.InternalPath;
import org.xbib.elasticsearch.search.aggregations.path.PathParser;

/**
 *
 */
public class AggregationPlugin extends Plugin {

    @Override
    public String name() {
        return "aggregations";
    }

    @Override
    public String description() {
        return "More aggregations";
    }

    public void onModule(SearchModule module) {
        module.registerAggregatorParser(PathParser.class);
        InternalPath.registerStreams();
    }
}
