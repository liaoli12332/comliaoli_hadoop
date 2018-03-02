package com.Secondary;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Administrator on 2018/3/1.
 */
public class ItemIdPartitioner extends Partitioner<OrderBean,NullWritable> {
    //相同id的订单，会发往相同的partition
    //而且，产生的分区数，是会跟用户设置的reduce task数保持一致
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks) {
        return (orderBean.getItemid().hashCode()&Integer.MAX_VALUE)%numReduceTasks;
    }
}
