package org.xbib.elasticsearch.search.aggregations.path;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

public class PathBuilder extends AggregationBuilder<PathBuilder> {

    private String separator = PathParser.DEFAULT_SEPARATOR;
    private int maxDepth = PathParser.DEFAULT_MAX_DEPTH;
    private int minDepth = PathParser.DEFAULT_MIN_DEPTH;
    private String field;
    private Integer depth;
    private Path.Order order;

    public PathBuilder(String name) {
        super(name, InternalPath.TYPE.name());
    }

    public PathBuilder field(String field) {
        this.field = field;
        return this;
    }

    public PathBuilder separator(String separator) {
        this.separator = separator;
        return this;
    }

    public PathBuilder minDepth(int minDepth) {
        this.minDepth = minDepth;
        return this;
    }

    public PathBuilder maxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
    }

    public PathBuilder depth(int depth) {
        this.depth = depth;
        return this;
    }

    public PathBuilder order(Path.Order order) {
        this.order = order;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (order != null) {
            builder.field("order");
            order.toXContent(builder, params);
        }
        if (!separator.equals(PathParser.DEFAULT_SEPARATOR)) {
            builder.field("separator", separator);
        }
        if (minDepth != PathParser.DEFAULT_MIN_DEPTH) {
            builder.field("min_depth", minDepth);
        }
        if (maxDepth != PathParser.DEFAULT_MAX_DEPTH) {
            builder.field("max_depth", maxDepth);
        }
        if (depth != null) {
            builder.field("depth", depth);
        }
        return builder.endObject();
    }
}
