package org.xbib.elasticsearch.search.aggregations.path;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

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
    @SuppressWarnings("unchecked")
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
            throw new ElasticsearchException("min_depth paramater must be lower than max_depth parameter");
        }
        return new PathAggregatorFactory(aggregationName, vsParser.config(), new BytesRef(separator), minDepth, maxDepth, order);
    }

}