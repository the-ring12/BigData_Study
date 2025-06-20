package com.the_ring.udaf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;



public class Avg extends Aggregator<Long, Buffer, Double> {


    @Override
    public Buffer zero() {
        return new Buffer(0L, 0L);
    }

    @Override
    public Buffer reduce(Buffer b, Long a) {
        b.setSum(b.getSum() + a);
        b.setCount(b.getCount() + 1);
        return b;
    }

    @Override
    public Buffer merge(Buffer b1, Buffer b2) {
        b1.setSum(b1.getSum() + b2.getSum());
        b1.setCount(b1.getCount() + b2.getCount());
        return b1;
    }

    @Override
    public Double finish(Buffer reduction) {
        return (double) reduction.getSum() / reduction.getCount();
    }

    @Override
    public Encoder<Buffer> bufferEncoder() {
        return Encoders.kryo(Buffer.class);
    }

    @Override
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }
}
