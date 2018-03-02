package com.enhance;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * maptask或者reducetask在最终输出时，先调用OutputFromat的getRecordWriter方法拿到一个RecordWriter
 *然后调用RecordWriter的write将数据写出
 * Created by Administrator on 2018/3/2.
 */
public class LogEnhanceOutputFormat extends FileOutputFormat<Text, NullWritable> {


    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path enhancePath = new Path("D:/temp/en/log.dat");
        Path tocrawlPath = new Path("D:/temp/crw/url.dat");
        FSDataOutputStream enhancedOs = fs.create(enhancePath);
        FSDataOutputStream tocrawlOs = fs.create(tocrawlPath);

        return new EnhanceRecordWriter(enhancedOs,tocrawlOs);
    }

    static class EnhanceRecordWriter extends RecordWriter<Text,NullWritable>{

        FSDataOutputStream enhancedOs=null;
        FSDataOutputStream tocrawlOs=null;

        public EnhanceRecordWriter(FSDataOutputStream enhancedOs, FSDataOutputStream tocrawlOs) {
            this.enhancedOs = enhancedOs;
            this.tocrawlOs = tocrawlOs;
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            String result = key.toString();
            if(result.contains("tocrawl")){
                //如果写出的数据是带爬的url，则写入带爬清单文件
                tocrawlOs.write(result.getBytes());
            }else{
                //如果要写出的数据是增强日志，则写入增强日志文件
                enhancedOs.write(result.getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if(tocrawlOs!=null){
                tocrawlOs.close();
            }
            if(enhancedOs!=null){
                enhancedOs.close();
            }
        }
    }
}
