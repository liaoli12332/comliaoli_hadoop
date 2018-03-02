package com.inverindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2018/3/1.
 */
public class InverIndexStepOne {

    static class InverIndexStepOneMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        Text k=new Text();

        IntWritable v=new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] words = line.split(" ");

            FileSplit inputSplit= (FileSplit) context.getInputSplit();
            String filename = inputSplit.getPath().getName();

            for(String word:words){
                k.set(word+"--"+filename);
                context.write(k,v);
            }

        }
    }

    static class InverIndexSteponeReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count=0;
            for(IntWritable value:values){
                count+=value.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf=new Configuration();

        Job job = Job.getInstance();
        job.setJarByClass(InverIndexStepOne.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(InverIndexStepOneMapper.class);
        job.setReducerClass(InverIndexSteponeReducer.class);

        FileInputFormat.setInputPaths(job,new Path("E:/wordcount/intervalinput"));
        FileOutputFormat.setOutputPath(job,new Path("E:/wordcount/intevaloutput"));
        job.waitForCompletion(true);
    }
}
