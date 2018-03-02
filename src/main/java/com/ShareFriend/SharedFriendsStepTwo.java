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
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by Administrator on 2018/3/1.
 */
public class SharedFriendsStepTwo {

    static class SharedFriendStepTwoMapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] friend_persons = line.split("\t");

            String friend = friend_persons[0];

            String[] persons = friend_persons[1].split(",");

            Arrays.sort(persons);

            for(int i=0;i<persons.length-2;i++){
                for(int j=i+1;j<persons.length-1;j++){
                    //这样相同的人人对的所有好友就会到reduce中去
                    context.write(new Text(persons[i]+"-"+persons[j]),new Text(friend));
                }
            }
        }
    }

    static class SharedFriendsStepTwoReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text person_person, Iterable<Text> friends, Context context) throws IOException, InterruptedException {

            StringBuffer sb=new StringBuffer();

            for(Text friend:friends){
                sb.append(friend).append(" ");
            }
            context.write(person_person,new Text(sb.toString()));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf=new Configuration();

        Job job = Job.getInstance();
        job.setJarByClass(SharedFriendsStepTwo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(SharedFriendStepTwoMapper.class);
        job.setReducerClass(SharedFriendsStepTwoReducer.class);

        FileInputFormat.setInputPaths(job,new Path("E:/wordcount/inverindexoutput-step1"));
        FileOutputFormat.setOutputPath(job,new Path("E:/wordcount/inverindexoutput-step2"));

        job.waitForCompletion(true);
    }
}
