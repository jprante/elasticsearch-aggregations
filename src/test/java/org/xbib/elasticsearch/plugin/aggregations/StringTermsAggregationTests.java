package org.xbib.elasticsearch.plugin.aggregations;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class StringTermsAggregationTests extends NodeTestUtils {

    private static final String SINGLE_VALUED_FIELD_NAME = "s_value";

    @Before
    public void setup() throws Exception {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            builders.add(client().prepareIndex("idx", "type").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val" + i).field("i", i)
                            .field("tag", i < 5 / 2 + 1 ? "more" : "less")
                            .endObject()));
        }
        for (IndexRequestBuilder builder : builders) {
            builder.setRefresh(true).execute().actionGet();
        }
    }

    @Test
    public void testStringTerms() {
        SearchResponse response = client()
                .prepareSearch("idx")
                .setTypes("type")
                .addAggregation(terms("terms").field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST)).execute().actionGet();
        assertNotNull(response);
        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));
        Object[] propertiesKeys = (Object[]) terms.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) terms.getProperty("_count");

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsString(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
            assertThat(propertiesKeys[i].toString(), equalTo("val" + i));
            assertThat(propertiesDocCounts[i].toString(), equalTo("1"));
        }
    }
}
