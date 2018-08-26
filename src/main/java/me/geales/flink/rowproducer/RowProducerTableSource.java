package me.geales.flink.rowproducer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class RowProducerTableSource implements StreamTableSource<Row>, DefinedProctimeAttribute, DefinedRowtimeAttributes {

    private final RowProducerSourceFunction rowProducerSourceFunction;
    private final int jitter;
    private final boolean outOfOrder;

    public RowProducerTableSource(long delay, int jitter, boolean outOfOrder) {
        this.jitter = jitter;
        this.outOfOrder = outOfOrder;
        this.rowProducerSourceFunction = new RowProducerSourceFunction(delay, jitter);
    }

    @Nullable
    @Override
    public String getProctimeAttribute() { return "proctime"; }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        List<RowtimeAttributeDescriptor> list = new ArrayList<>();
        list.add(new RowtimeAttributeDescriptor(
                "rowtime",
                new StreamRecordTimestamp(),
                outOfOrder ? new AscendingTimestamps() : new BoundedOutOfOrderTimestamps(jitter * 2)
        ));
        return list;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        return streamExecutionEnvironment.addSource(
                this.rowProducerSourceFunction)
                .name("RowProducer")
                .returns(Types.ROW(Types.LONG, Types.STRING, Types.SQL_TIMESTAMP, Types.INT));
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return Types.ROW_NAMED(new String[]{"id", "type", "generatedAt", "val"}, Types.LONG, Types.STRING, Types.SQL_TIMESTAMP, Types.INT);
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .field("id", Types.LONG)
                .field("type", Types.STRING)
                .field("generatedAt", Types.SQL_TIMESTAMP)
                .field("val", Types.INT)
                .field("proctime", Types.SQL_TIMESTAMP)
                .field("rowtime", Types.SQL_TIMESTAMP)
                .build();
    }

    @Override
    public String explainSource() {
        return "RowProducerSourceFactory";
    }
}
