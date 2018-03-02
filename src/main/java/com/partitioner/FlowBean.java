package com.partitioner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2018/2/26.
 */
public class FlowBean implements Writable {

    private long upFlow;
    private long dFlow;
    private long sumFlow;

    //反序列化时，要调用空参构造
    public FlowBean(){

    }

    public FlowBean(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow=upFlow+dFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }




    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public void setdFlow(long dFlow) {
        this.dFlow = dFlow;
    }

    /**
     *序列化的方法
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(dFlow);
        out.writeLong(sumFlow);
    }

    /**
     *反序列化的方法
     * 注意反序列化的顺序和序列化的顺序完全一致
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        dFlow=in.readLong();
        sumFlow=in.readLong();
    }

    @Override
    public String toString() {
        return upFlow+"\t"+dFlow+"\t"+sumFlow;
    }
}
