package me.geales.flink.eventproducer;

import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventProducerTableFactory implements StreamTableSourceFactory<Row> {
    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> requiredContextMap = new HashMap<>();
        requiredContextMap.put("connector.type", "test-source");
        return requiredContextMap;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> list = new ArrayList<>();
        list.add("connector.debug");
        return list;
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map map) {
        return new EventProducerTableSource();
    }

}
