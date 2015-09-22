package org.xbib.elasticsearch.search.aggregations.path;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class InternalPath
        extends InternalMultiBucketAggregation<InternalPath,InternalPath.Bucket>
        implements Path, ToXContent, Streamable {

    private final static ESLogger logger = ESLoggerFactory.getLogger(InternalPath.class.getName());

    public static final Type TYPE = new Type("path");

    protected Map<BytesRef, Path.Bucket> bucketMap;

    private List<InternalPath.Bucket> buckets;

    private Path.Order order;

    private BytesRef separator;

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalPath readResult(StreamInput in) throws IOException {
            InternalPath buckets = new InternalPath();
            buckets.readFrom(in);
            return buckets;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            InternalPath.Bucket buckets = new InternalPath.Bucket();
            buckets.readFrom(in);
            return buckets;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            Map<String, Object> attributes = new HashMap<>();
            context.attributes(attributes);
            return context;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    InternalPath() {
    }

    public InternalPath(String name,
                           List<PipelineAggregator> pipelineAggregators,
                           Map<String, Object> metaData,
                           List<InternalPath.Bucket> buckets,
                           Path.Order order,
                           BytesRef separator) {
        super(name, pipelineAggregators, metaData);
        this.buckets = buckets;
        this.order = order;
        this.separator = separator;
        logger.info("new: {}", buckets);
    }

    @Override
    public InternalPath create(List<InternalPath.Bucket> buckets) {
        return new InternalPath(this.name, this.pipelineAggregators(), this.metaData,
                buckets, this.order, this.separator);
    }

    @Override
    public InternalPath.Bucket createBucket(InternalAggregations aggregations, InternalPath.Bucket prototype) {
        return new Bucket(prototype.val, prototype.termBytes, prototype.docCount, aggregations, prototype.level, prototype.path);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public List<Path.Bucket> getBuckets() {
        Object o = buckets;
        return (List<Path.Bucket>) o;
    }

    @Override
    public Path.Bucket getBucketByKey(String path) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>();
            for (Path.Bucket bucket : buckets) {
                bucketMap.put(((Bucket) bucket).termBytes, bucket);
            }
        }
        return bucketMap.get(new BytesRef(path));
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Map<BytesRef, List<InternalPath.Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalPath p = (InternalPath) aggregation;
            if (buckets == null) {
                buckets = new HashMap<>();
            }
            for (InternalPath.Bucket bucket : p.buckets) {
                List<InternalPath.Bucket> existingBuckets = buckets.get(bucket.termBytes);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.termBytes, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }
        List<InternalPath.Bucket> reduced = (buckets != null) ?
                new ArrayList<>(buckets.size()) :
                new ArrayList<>();
        if (buckets != null) {
            for (Map.Entry<BytesRef, List<InternalPath.Bucket>> entry : buckets.entrySet()) {
                List<InternalPath.Bucket> sameCellBuckets = entry.getValue();
                reduced.add(sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext));
            }
        }
        Map<String, List<InternalPath.Bucket>> res = new HashMap<>();
        for (InternalPath.Bucket bucket : reduced) {
            String key = bucket.path.length > 0 ?
                    String.join(separator.utf8ToString(), bucket.path) :
                    separator.utf8ToString();
            List<InternalPath.Bucket> listBuckets = res.get(key);
            if (listBuckets == null) {
                listBuckets = new ArrayList<>();
            }
            listBuckets.add(bucket);
            res.put(key, listBuckets);
        }
        for (List<InternalPath.Bucket> bucket : res.values()) {
            CollectionUtil.introSort(bucket, order.comparator());
        }
        return new InternalPath(getName(), pipelineAggregators(), metaData,
                createBucketListFromMap(res),
                order, separator);
    }

    private List<InternalPath.Bucket> createBucketListFromMap(Map<String, List<InternalPath.Bucket>> buckets) {
        List<InternalPath.Bucket> res = new ArrayList<>();
        if (buckets.size() > 0) {
            List<InternalPath.Bucket> rootList = buckets.get(separator.utf8ToString());
            createBucketListFromMapRecurse(res, buckets, rootList);
        }
        return res;
    }

    private void createBucketListFromMapRecurse(List<InternalPath.Bucket> res,
                                                Map<String, List<InternalPath.Bucket>> mapBuckets,
                                                List<InternalPath.Bucket> buckets) {
        for (InternalPath.Bucket bucket : buckets) {
            res.add(bucket);
            List<InternalPath.Bucket> children = mapBuckets.get(bucket.getKey());
            if (children != null && !children.isEmpty()) {
                createBucketListFromMapRecurse(res, mapBuckets, children);
            }
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        Iterator<? extends Path.Bucket> bucketIterator = buckets.iterator();
        builder.startArray(CommonFields.BUCKETS);
        if (bucketIterator.hasNext()) {
            Path.Bucket firstBucket = bucketIterator.next();
            doXContentInternal(builder, params, firstBucket, bucketIterator);
        }
        builder.endArray();
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalPath.Order o = (Order) order;
        out.writeByte(o.id());
        if (order instanceof Aggregation) {
            out.writeBoolean(o.asc());
            out.writeString(o.key());
        }
        out.writeBytesRef(separator);
        out.writeVInt(buckets.size());
        for (Path.Bucket pathbucket : buckets) {
            Bucket bucket = (Bucket) pathbucket;
            out.writeString(bucket.val);
            out.writeBytesRef(bucket.termBytes);
            out.writeLong(bucket.docCount);
            ((InternalAggregations) bucket.getAggregations()).writeTo(out);
            out.writeInt(bucket.level);
            out.writeInt(bucket.path.length);
            for (String path : bucket.path) {
                out.writeString(path);
            }
        }
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        byte id = in.readByte();
        switch (id) {
            case 1:
                order = (Order) Order.KEY_ASC;
                break;
            case 2:
                order = (Order) Order.KEY_DESC;
                break;
            case 3:
                order = (Order) Order.COUNT_ASC;
                break;
            case 4:
                order = (Order) Order.COUNT_DESC;
                break;
            case 0:
                boolean asc = in.readBoolean();
                String key = in.readString();
                order = new Aggregation(key, asc);
                break;
            default:
                throw new IllegalArgumentException("undefined path order");
        }
        separator = in.readBytesRef();
        int listSize = in.readVInt();
        this.buckets = new ArrayList<>(listSize);
        for (int i = 0; i < listSize; i++) {
            Bucket bucket = new Bucket(in.readString(), in.readBytesRef(), in.readLong(), InternalAggregations.readAggregations(in), in.readInt(), null);
            int sizePath = in.readInt();
            String[] paths = new String[sizePath];
            for (int k = 0; k < sizePath; k++) {
                paths[k] = in.readString();
            }
            bucket.path = paths;
            buckets.add(bucket);
        }
        this.bucketMap = null;
    }

    private void doXContentInternal(XContentBuilder builder, Params params, Path.Bucket currentBucket,
                                    Iterator<? extends Path.Bucket> bucketIterator) throws IOException {
        builder.startObject();
        builder.field(CommonFields.KEY, ((Bucket) currentBucket).val);
        builder.field(CommonFields.DOC_COUNT, currentBucket.getDocCount());
        ((InternalAggregations) currentBucket.getAggregations()).toXContentInternal(builder, params);
        Bucket bucket = (Bucket) currentBucket;
        if (bucketIterator.hasNext()) {
            Bucket nextBucket = (Bucket) bucketIterator.next();
            if (nextBucket.level == bucket.level) {
                builder.endObject();
            } else if (nextBucket.level > bucket.level) {
                builder.startObject(name);
                builder.startArray(CommonFields.BUCKETS);
            } else {
                builder.endObject();
                for (int i = bucket.level; i > nextBucket.level; i--) {
                    builder.endArray();
                    builder.endObject();
                    builder.endObject();
                }
            }
            doXContentInternal(builder, params, nextBucket, bucketIterator);
        } else {
            if (bucket.level > 0) {
                builder.endObject();
                for (int i = bucket.level; i > 0; i--) {
                    builder.endArray();
                    builder.endObject();
                    builder.endObject();
                }
            } else {
                builder.endObject();
            }
        }
    }

    static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Path.Bucket {

        protected long docCount;
        protected InternalAggregations aggregations;
        protected int level;
        protected String[] path;
        protected String val;
        BytesRef termBytes;

        public Bucket() {
        }

        public Bucket(String val, BytesRef term, long docCount, InternalAggregations aggregations, int level, String[] path) {
            this.termBytes = term;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.level = level;
            this.path = path;
            this.val = val;
            logger.info("{} val={}", this, val);
        }

        @Override
        public String getKey() {
            return termBytes.utf8ToString();
        }

        @Override
        public String getKeyAsString() {
            return termBytes.utf8ToString();
        }

        @Override
        public int compareTerm(Path.Bucket other) {
            return BytesRef.getUTF8SortedAsUnicodeComparator().compare(termBytes, ((Bucket) other).termBytes);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public Object getProperty(String containingAggName, List<String> path) {
            return null;
        }

        public InternalPath.Bucket reduce(List<InternalPath.Bucket> buckets, ReduceContext reduceContext) {
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            InternalPath.Bucket reduced = null;
            for (InternalPath.Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            if (reduced != null) {
                reduced.aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
            }
            return reduced;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            termBytes = in.readBytesRef();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readAggregations(in);
            val = in.readString();
            level = in.readInt();
            int len = in.readInt();
            path = new String[len];
            if (len > 0) {
                for (int i = 0; i < len; i++) {
                    path[i] = in.readString();
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesRef(termBytes);
            out.writeVLong(getDocCount());
            aggregations.writeTo(out);
            out.writeString(val);
            out.writeInt(level);
            if (path == null) {
                out.writeInt(0);
            } else {
                out.writeInt(path.length);
                for (String p : path) {
                    out.writeString(p);
                }
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.utf8Field(CommonFields.KEY, termBytes);
            builder.field(CommonFields.DOC_COUNT, getDocCount());
            aggregations.toXContentInternal(builder, params);
            builder.field("val", val);
            builder.field("level", level);
            builder.array("path", path);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            XContentBuilder builder;
            try {
                builder = jsonBuilder();
                return toXContent(builder, ToXContent.EMPTY_PARAMS).string();
            } catch (Exception e) {
                return "?";
            }
        }
    }

    static class Order extends Path.Order {

        private final byte id;
        private final String key;
        private final boolean asc;
        private final Comparator<Path.Bucket> comparator;

        Order(byte id, String key, boolean asc, Comparator<Path.Bucket> comparator) {
            this.id = id;
            this.key = key;
            this.asc = asc;
            this.comparator = comparator;
        }

        byte id() {
            return id;
        }

        String key() {
            return key;
        }

        boolean asc() {
            return asc;
        }

        @Override
        Comparator<Path.Bucket> comparator() {
            return comparator;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field(key, asc ? "asc" : "desc").endObject();
        }
    }

    static class Aggregation extends Order {

        private static final byte ID = 0;

        Aggregation(String key, boolean asc) {
            super(ID, key, asc, new MultiBucketsAggregation.Bucket.SubAggregationComparator<Path.Bucket>(key, asc));
        }
    }

}