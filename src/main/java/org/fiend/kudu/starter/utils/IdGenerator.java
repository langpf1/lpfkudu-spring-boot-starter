package org.fiend.kudu.starter.utils;

/**
 * IdGenerator
 */
public class IdGenerator {
    // Mon Mar 12 15:41:17 CST 2018
    // 日期起始点
    private final static long twepoch = 1520840477347L;

    private final long workerId;
    private volatile long sequence = 0L;
    private volatile long lastTimestamp = -1L;

    // 机器ID占用10bits
    private final static long workerIdBits = 10L;

    // 序列占用12bits
    private final static long sequenceBits = 12L;

    // 机器ID 最大值
    public final static long maxWorkerId = -1L ^ -1L << workerIdBits;

    //时间偏移位
    private final static long timestampLeftShift = sequenceBits + workerIdBits;
    // 机器ID偏移位
    private final static long workerIdShift = sequenceBits;

    // 序列掩码
    public final static long sequenceMask = -1L ^ -1L << sequenceBits;


    public IdGenerator(final long workerId) {
        super();
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        this.workerId = workerId;
    }

    public synchronized long nextId() {
        long timestamp = this.timeGen();

        if (this.lastTimestamp == timestamp) {
            this.sequence = (this.sequence + 1) & sequenceMask;
            if (this.sequence == 0) {
                timestamp = this.tilNextMillis(this.lastTimestamp);
            }
        } else {
            this.sequence = 0;
        }

        if (timestamp < this.lastTimestamp) {
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", this.lastTimestamp - timestamp));
        }

        this.lastTimestamp = timestamp;
        long nextId = ((timestamp - twepoch << timestampLeftShift)) | (this.workerId << workerIdShift) | (this.sequence);

        return nextId;
    }

    private long tilNextMillis(final long lastTimestamp) {
        long timestamp = this.timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = this.timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }
}
