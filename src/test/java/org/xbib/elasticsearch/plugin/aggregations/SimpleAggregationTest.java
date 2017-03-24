package org.xbib.elasticsearch.plugin.aggregations;

import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.xbib.elasticsearch.search.aggregations.path.Path;
import org.xbib.elasticsearch.search.aggregations.path.PathBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class SimpleAggregationTest extends NodeTestUtils {

    private final static ESLogger logger = ESLoggerFactory.getLogger(SimpleAggregationTest.class.getName());

    @Test
    public void simpleTest() throws IOException {
        Client client = client();

        CreateIndexRequestBuilder createIndexRequestBuilder = new CreateIndexRequestBuilder(client, CreateIndexAction.INSTANCE, "test");
        Settings settings = Settings.builder()
                .put("index.analysis.analyzer.default.type", "keyword")
                .build();
        createIndexRequestBuilder.setSettings(settings).execute().actionGet();

        BulkRequestBuilder builder = new BulkRequestBuilder(client, BulkAction.INSTANCE);
        for (int i = 0; i < 100; i++) {
            builder.add(indexRequest()
                    .index("test").type("docs")
                    .source(jsonBuilder()
                            .startObject()
                            .field("title", "My title " + (i/4))
                            .startObject("author")
                            .field("name", "My author name " + (i/4))
                            .endObject()
                            .field("subject", "/docs/title" + (i/4))
                            .endObject()));
        }
        for (int i = 0; i < 100; i++) {
            builder.add(indexRequest()
                    .index("test").type("docs")
                    .source(jsonBuilder()
                            .startObject()
                            .array("subject", "/1546#BSA/986#BCM/985#CFM/1320#F1M/807#GP IIb IIIa M/402#GP IIb IIIa A",
                                    "/1546#BSA/986#BCM/1445#PAM/312#PAI",
                                    "/965#CA/945#VA/1445#PAM/312#PAI",
                                    "/15588#PT/8145#SPM/985#CFM/1320#F1M/807#GP IIb IIIa M/402#GP IIb IIIa A")
                            .endObject()
                    )
            );
        }
        client.bulk(builder.request()).actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client(), SearchAction.INSTANCE);
        searchRequestBuilder.setQuery(QueryBuilders.constantScoreQuery(matchAllQuery()))
                .addAggregation(new PathBuilder("subject")
                        .field("subject")
                        .maxDepth(2)
                        .separator("/")
                        .order(Path.Order.KEY_DESC)
                );

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        Path path = searchResponse.getAggregations().get("subject");
        List<Path.Bucket> buckets = path.getBuckets();
        Iterator<Path.Bucket> iterator = buckets.iterator();
        InputStream inputStream = getClass().getClassLoader().getResource("path-aggs.txt").openStream();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                assertEquals(line, iterator.next().toString());
            }
        }
    }
}
