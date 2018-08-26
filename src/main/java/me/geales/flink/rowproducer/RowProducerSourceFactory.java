package me.geales.flink.rowproducer;

import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowProducerSourceFactory implements StreamTableSourceFactory<Row> {
    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> map) {
        long delay = Long.parseLong(map.getOrDefault("connector.delay", "500"));
        int jitter = Integer.parseInt(map.getOrDefault("connector.jitter", "0"));
        boolean outOfOrder = map.containsKey("connector.outOfOrderDelay");
        int outOfOrderDelay = Integer.parseInt(map.getOrDefault("connector.outOfOrderDelay", "600"));
        return new RowProducerTableSource(delay, jitter, outOfOrder, outOfOrderDelay);
    }

    public Map<String, String> requiredContext() {
        Map<String, String> requiredContextMap = new HashMap<>();
        requiredContextMap.put("connector.type", "test-source");
        return requiredContextMap;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> list = new ArrayList<>();
        list.add("connector.debug");
        list.add("connector.delay");
        list.add("connector.jitter");
        list.add("connector.outOfOrder");
        return list;
    }
}
