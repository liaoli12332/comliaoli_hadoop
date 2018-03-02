package com.Secondary;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2018/3/1.
 */
public class SecondarySort {

    static class SecondarySortMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {

        OrderBean bean=new OrderBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = StringUtils.split(line, ",");

            bean.set(new Text(fields[0]),new DoubleWritable(Double.parseDouble(fields[2])));

            context.write(bean,NullWritable.get());
        }
    }

    static class SecondarySortReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{

        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf=new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(SecondarySort.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("E:/wordcount/gpinput"));
        FileOutputFormat.setOutputPath(job,new Path("E:/wordcount/gpoutput"));

        job.setGroupingComparatorClass(ItemidGroupingComparator.class);
        job.setPartitionerClass(ItemIdPartitioner.class);

        job.setNumReduceTasks(2);

        job.waitForCompletion(true);


    }

}