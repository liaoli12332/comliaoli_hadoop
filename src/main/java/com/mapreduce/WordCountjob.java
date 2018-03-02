package com.mapreduce;


import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * Created by Administrator on 2018/2/26.
 */
public class WordCountjob {

    private static Logger logger= LogManager.getLogger("WordCountJob");

    public static void main(String[] args) {
        logger.info("WordCountJob开始运作");
        Configuration conf = new Configuration();
        conf.addResource(Resources.getResource("core-site.xml"));
        //使用本地模式跑mapreduce
        //conf.set("mapreduce.framework.name","local");
        //conf.set("fs.defaultFS","file:///");

        try {
            Job job = Job.getInstance(conf, "WordCount");
            job.setJarByClass(WordCountjob.class);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job,new Path("/wordcount/input"));
            FileOutputFormat.setOutputPath(job,new Path("/wordcount/output"));
            boolean flag = job.waitForCompletion(true);
            logger.info("WordCountJob结束运作，运行结果："+flag);
        } catch (IOException|InterruptedException|ClassNotFoundException e) {
            logger.error("WordCountJob结束失败");
            logger.error("WordCountJob.main",e);
        }

    }

}
