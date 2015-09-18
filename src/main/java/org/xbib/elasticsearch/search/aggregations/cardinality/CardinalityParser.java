package org.xbib.elasticsearch.search.aggregations.cardinality;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public class CardinalityParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalCardinality.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String name, XContentParser parser, SearchContext context) throws IOException {
        ValuesSourceParser vsParser = ValuesSourceParser.any(name, InternalCardinality.TYPE, context).formattable(false).build();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else {
                throw new SearchParseException(context, "unexpected token " + token + " in [" + name + "].");
            }
        }
        ValuesSourceConfig<?> config = vsParser.config();
        return new CardinalityAggregatorFactory(name, config);
    }

}
