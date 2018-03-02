package com.ShareFriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class SharedFriendsStepOne {

    static class SharedFriendStepOneMapper extends Mapper<LongWritable,Text,Text,Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] person_friends = line.split(":");
            String person = person_friends[0];
            String[] friends = person_friends[1].split(",");

            for(String friend:friends){
                //输出格式 好友，人
                context.write(new Text(friend),new Text(person));

            }

        }
    }

    static class SharedFriendsStepOneReducer extends Reducer<Text,Text,Text,Text> {


        @Override
        protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {

            StringBuffer sb=new StringBuffer();

            for(Text person:persons){

                sb.append(person).append(",");
            }
            context.write(friend,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf=new Configuration();

        Job job = Job.getInstance();
        job.setJarByClass(SharedFriendsStepOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SharedFriendStepOneMapper.class);
        job.setReducerClass(SharedFriendsStepOneReducer.class);

        FileInputFormat.setInputPaths(job,new Path("E:/wordcount/input/1111.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:/wordcount/inverindexoutput-step1"));

        job.waitForCompletion(true);
    }
}