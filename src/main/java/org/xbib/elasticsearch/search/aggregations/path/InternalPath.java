package org.xbib.elasticsearch.search.aggregations.path;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InternalPath extends InternalAggregation implements Path {

    public static final Type TYPE = new Type("path");
    protected Map<BytesRef, Path.Bucket> bucketMap;
    private List<Path.Bucket> buckets;
    private Order order;
    private BytesRef separator;

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalPath readResult(StreamInput in) throws IOException {
            InternalPath buckets = new InternalPath();
            buckets.readFrom(in);
            return buckets;
        }
    };

    InternalPath() {
    }

    public InternalPath(String name, List<Path.Bucket> buckets, Order order, BytesRef separator) {
        super(name);
        this.buckets = buckets;
        this.order = order;
        this.separator = separator;
    }

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public List<Path.Bucket> getBuckets() {
        return buckets;
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
    public InternalPath reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        Map<BytesRef, List<Path.Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalPath pathHierarchy = (InternalPath) aggregation;
            if (buckets == null) {
                buckets = new HashMap<>();
            }
            for (Path.Bucket bucket : pathHierarchy.buckets) {
                List<Path.Bucket> existingBuckets = buckets.get(((Bucket) bucket).termBytes);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(((Bucket) bucket).termBytes, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }
        List<Path.Bucket> reduced = (buckets != null) ?
                new ArrayList<Path.Bucket>(buckets.size()) : new ArrayList<Path.Bucket>();
        if (buckets != null) {
            for (Map.Entry<BytesRef, List<Path.Bucket>> entry : buckets.entrySet()) {
                List<Path.Bucket> sameCellBuckets = entry.getValue();
                reduced.add(((Bucket) sameCellBuckets.get(0)).reduce(sameCellBuckets, reduceContext));
            }
        }
        Map<String, List<Path.Bucket>> res = new HashMap<>();
        for (Path.Bucket bucket : reduced) {
            String key = ((Bucket) bucket).path.length > 0 ?
                    StringUtils.join(((Bucket) bucket).path, separator.utf8ToString()) : separator.utf8ToString();
            List<Path.Bucket> listBuckets = res.get(key);
            if (listBuckets == null) {
                listBuckets = new ArrayList<>();
            }
            listBuckets.add(bucket);
            res.put(key, listBuckets);
        }
        for (List<Path.Bucket> bucket : res.values()) {
            CollectionUtil.introSort(bucket, order.comparator());
        }
        return new InternalPath(getName(), createBucketListFromMap(res), order, separator);
    }

    private List<Path.Bucket> createBucketListFromMap(Map<String, List<Path.Bucket>> buckets) {
        List<Path.Bucket> res = new ArrayList<>();
        if (buckets.size() > 0) {
            List<Path.Bucket> rootList = buckets.get(separator.utf8ToString());
            createBucketListFromMapRecurse(res, buckets, rootList);
        }
        return res;
    }

    private void createBucketListFromMapRecurse(List<Path.Bucket> res,
                                                Map<String, List<Path.Bucket>> mapBuckets,
                                                List<Path.Bucket> buckets) {
        for (Path.Bucket bucket : buckets) {
            res.add(bucket);
            List<Path.Bucket> children = mapBuckets.get(bucket.getKey());
            if (children != null && !children.isEmpty()) {
                createBucketListFromMapRecurse(res, mapBuckets, children);
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        order = Streams.readOrder(in);
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        Streams.writeOrder(order, out);
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
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        Iterator<Path.Bucket> bucketIterator = buckets.iterator();
        builder.startArray(CommonFields.BUCKETS);
        if (bucketIterator.hasNext()) {
            Path.Bucket firstBucket = bucketIterator.next();
            doXContentInternal(builder, params, firstBucket, bucketIterator);
        }
        builder.endArray();
        return builder;
    }

    private void doXContentInternal(XContentBuilder builder, Params params, Path.Bucket currentBucket,
                                    Iterator<Path.Bucket> bucketIterator) throws IOException {
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

    static class Bucket implements Path.Bucket {

        protected long docCount;
        protected InternalAggregations aggregations;
        protected int level;
        protected String[] path;
        protected String val;
        BytesRef termBytes;

        public Bucket(String val, BytesRef term, long docCount, InternalAggregations aggregations, int level, String[] path) {
            this.termBytes = term;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.level = level;
            this.path = path;
            this.val = val;
        }

        @Override
        public String getKey() {
            return termBytes.utf8ToString();
        }

        @Override
        public Text getKeyAsText() {
            return new BytesText(new BytesArray(termBytes));
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

        public Path.Bucket reduce(List<? extends Path.Bucket> buckets, ReduceContext reduceContext) {
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            Path.Bucket reduced = null;
            for (Path.Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    ((Bucket) reduced).docCount += ((Bucket) bucket).docCount;
                }
                aggregationsList.add(((Bucket) bucket).aggregations);
            }
            if (reduced != null) {
                ((Bucket) reduced).aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
            }
            return reduced;
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

    static class Streams {

        public static void writeOrder(Order order, StreamOutput out) throws IOException {
            out.writeByte(order.id());
            if (order instanceof Aggregation) {
                out.writeBoolean(order.asc());
                out.writeString(order.key());
            }
        }

        public static Order readOrder(StreamInput in) throws IOException {
            byte id = in.readByte();
            switch (id) {
                case 1:
                    return (Order) Order.KEY_ASC;
                case 2:
                    return (Order) Order.KEY_DESC;
                case 3:
                    return (Order) Order.COUNT_ASC;
                case 4:
                    return (Order) Order.COUNT_DESC;
                case 0:
                    boolean asc = in.readBoolean();
                    String key = in.readString();
                    return new Aggregation(key, asc);
                default:
                    throw new IllegalArgumentException("undefined path order");
            }
        }
    }
}