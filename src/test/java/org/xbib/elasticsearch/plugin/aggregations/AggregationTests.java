package org.xbib.elasticsearch.plugin.aggregations;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.hppc.ObjectIntMap;
import org.elasticsearch.common.hppc.ObjectIntOpenHashMap;
import org.junit.Test;
import org.xbib.elasticsearch.helper.AbstractNodesTestHelper;
import org.xbib.elasticsearch.search.aggregations.path.Path;
import org.xbib.elasticsearch.search.aggregations.path.PathBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationTests extends AbstractNodesTestHelper {

    private static final String PATH_FIELD_NAME = "path";
    private static final String VIEWS_FIELD_NAME = "views";

    @Test
    public void testPath() throws IOException {
        client("1").admin().indices().prepareCreate("idx")
                .addMapping("path", PATH_FIELD_NAME, "type=string,index=not_analyzed")
                .addMapping("path2", PATH_FIELD_NAME, "type=string,index=not_analyzed")
                .execute().actionGet();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client("1").prepareIndex("idx", "path").setSource(jsonBuilder()
                .startObject()
                .field(PATH_FIELD_NAME, "/My documents/Spreadsheets/Budget_2013.xls")
                .field(VIEWS_FIELD_NAME, 10)
                .endObject()));
        builders.add(client("1").prepareIndex("idx", "path").setSource(jsonBuilder()
                .startObject()
                .field(PATH_FIELD_NAME, "/My documents/Spreadsheets/Budget_2014.xls")
                .field(VIEWS_FIELD_NAME, 7)
                .endObject()));
        builders.add(client("1").prepareIndex("idx", "path").setSource(jsonBuilder()
                .startObject()
                .field(PATH_FIELD_NAME, "/My documents/Test.txt")
                .field(VIEWS_FIELD_NAME, 1)
                .endObject()));
        for (IndexRequestBuilder builder : builders) {
            builder.setRefresh(true).execute().actionGet();
        }
        SearchResponse response = client("1").prepareSearch("idx").setTypes("path")
                .addAggregation(new PathBuilder("path")
                                .field(PATH_FIELD_NAME)
                                .separator("/")
                ).execute().actionGet();
        Path path = response.getAggregations().get("path");
        List<Path.Bucket> buckets = path.getBuckets();
        assertTrue(buckets.size() > 0);

        ObjectIntMap<String> expectedDocCountsForPath  = new ObjectIntOpenHashMap<>();
        expectedDocCountsForPath.put("My documents", 3);
        expectedDocCountsForPath.put("My documents/Spreadsheets", 2);
        expectedDocCountsForPath.put("My documents/Spreadsheets/Budget_2013.xls", 1);
        expectedDocCountsForPath.put("My documents/Spreadsheets/Budget_2014.xls", 1);
        expectedDocCountsForPath.put("My documents/Test.txt", 1);

        for (Path.Bucket bucket: buckets) {
            assertEquals(expectedDocCountsForPath.get(bucket.getKey()), bucket.getDocCount());
        }

    }
}
