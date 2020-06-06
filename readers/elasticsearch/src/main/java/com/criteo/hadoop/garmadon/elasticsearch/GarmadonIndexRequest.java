package com.criteo.hadoop.garmadon.elasticsearch;

import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.Map;

public class GarmadonIndexRequest extends IndexRequest {
    static private CommittableOffset<String, byte[]> committableOffset;

    public GarmadonIndexRequest(String index, CommittableOffset committableOffset) {
        super(index);
        this.committableOffset = committableOffset;
    }

    public GarmadonIndexRequest source(Map<String, ?> source) throws ElasticsearchGenerationException {
        source(source, Requests.INDEX_CONTENT_TYPE);
        return this;
    }

    public static CommittableOffset getCommittableOffset() {
        return committableOffset;
    }
}
