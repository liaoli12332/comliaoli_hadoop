package com.Secondary;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2018/3/1.
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private Text itemid;

    private DoubleWritable amount;

    public OrderBean(){

    }

    public void set(Text itemid, DoubleWritable amount) {
        this.itemid = itemid;
        this.amount = amount;
    }

    public Text getItemid() {
        return itemid;
    }

    public void setItemid(Text itemid) {
        this.itemid = itemid;
    }

    public DoubleWritable getAmount() {
        return amount;
    }

    public void setAmount(DoubleWritable amount) {
        this.amount = amount;
    }

    @Override
    public int compareTo(OrderBean o) {
        int cmp=this.itemid.compareTo(o.getItemid());
        if(cmp==0){
            cmp=-this.amount.compareTo(o.getAmount());
        }
        return cmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(itemid.toString());
        out.writeDouble(amount.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String readUTF = in.readUTF();
        double readDouble = in.readDouble();

        this.itemid=new Text(readUTF);
        this.amount=new DoubleWritable(readDouble);
    }

    @Override
    public String toString() {
        return itemid.toString()+"\t"+amount.get();
    }
}
