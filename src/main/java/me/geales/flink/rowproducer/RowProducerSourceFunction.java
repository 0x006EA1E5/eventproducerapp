package me.geales.flink.rowproducer;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RowProducerSourceFunction implements SourceFunction<Row> {

    private volatile boolean isRunning = true;
    private final long delay;
    private final int jitter;
    private final Random random = new Random();
    private final AtomicLong counter = new AtomicLong();

    public RowProducerSourceFunction(long delay, int jitter) {
        this.delay = delay;
        this.jitter = jitter;
    }
    @Override
    public void run(SourceFunction.SourceContext<Row> sourceContext) throws Exception {
        while(this.isRunning) {
            try {
                TimeUnit.MILLISECONDS.sleep(this.delay);
            } catch (InterruptedException e) {
                System.err.print("Cancelled");
                throw e;
            }


            long timeLong = Instant.now().toEpochMilli() + random.nextInt(jitter);
            Row row = new Row(3);
            row.setField(0, this.counter.getAndIncrement());
            row.setField(1, "" + (char)((int)'A' + random.nextInt(24)));
            row.setField(2, new Timestamp(timeLong));
//            row.setField(3, random.nextInt(99));
            sourceContext.collectWithTimestamp(row, timeLong);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
