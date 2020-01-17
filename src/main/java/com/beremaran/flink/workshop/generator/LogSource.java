package com.beremaran.flink.workshop.generator;

import com.beremaran.flink.workshop.factory.LogFactory;
import com.beremaran.flink.workshop.model.Log;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.atomic.AtomicBoolean;

public class LogSource implements SourceFunction<Log> {
    private final long batchSize;
    private final long batchInterval;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public LogSource(long batchSize, long batchInterval) {
        this.batchSize = batchSize;
        this.batchInterval = batchInterval;
    }

    @Override
    public void run(SourceContext<Log> sourceContext) throws Exception {
        while (running.get()) {
            for (long i = 0; i < batchSize; i++) {
                sourceContext.collect(LogFactory.create());
            }

            Thread.sleep(batchInterval);
        }
    }

    @Override
    public void cancel() {
        running.set(false);
    }
}
