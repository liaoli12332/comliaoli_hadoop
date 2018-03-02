package com.Secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2018/3/1.
 */
public class ItemidGroupingComparator extends WritableComparator{

    protected ItemidGroupingComparator(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean abean=(OrderBean)a;
        OrderBean bbean=(OrderBean)b;
        return abean.getItemid().compareTo(bbean.getItemid());
    }
}
