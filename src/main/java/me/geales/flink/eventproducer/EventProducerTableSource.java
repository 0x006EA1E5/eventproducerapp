package me.geales.flink.eventproducer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EventProducerTableSource implements StreamTableSource<Row>, DefinedProctimeAttribute, DefinedRowtimeAttributes {

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        return streamExecutionEnvironment
                .addSource(new EventProducerSourceFunction(), "eventProducer")
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Map<String, String>>(){

                    @Override
                    public long extractAscendingTimestamp(Map<String, String> o) {
                        return Long.parseLong(o.get("generatedAt"));
                    }
                })
//                    .map(map -> new Tuple3<>(Long.parseLong(map.get("id")), map.get("type"), Long.parseLong(map.get("generatedAt"))));
        .map(map -> {
            Row row = new Row(map.size());
            int i = 0;
            for (String s : map.keySet()) {
                row.setField(i, s);
                ++i;
            }
            return row;
        });
    }

    @Override
    public TypeInformation getReturnType() {
        return Types.ROW_NAMED(new String[]{"dataMap", "f1", "f2"}, Types.STRING, Types.STRING, Types.STRING);
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .field("dataMap", Types.STRING)
                .field("proctime", Types.SQL_TIMESTAMP)
                .field("rowtime", Types.SQL_TIMESTAMP)
                .build();

    }

    @Override
    public String explainSource() {
        return "EventProducer";
    }

    @Nullable
    @Override
    public String getProctimeAttribute() {
        return "proctime";
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {


        List<RowtimeAttributeDescriptor> list = new ArrayList<>();
        list.add(new RowtimeAttributeDescriptor("rowtime", new StreamRecordTimestamp(), new AscendingTimestamps()));
        return list;
    }
}
