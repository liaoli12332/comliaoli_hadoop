package com.enhance;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/3/1.
 */
public class LogEnhance {


    static class LogEnhanceMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

        Map<String,String> ruleMap=new HashMap<String,String>();

        Text k=new Text();

        NullWritable v=NullWritable.get();

        //从数据库中加载规则信息到ruleMap中
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try{
                DbLoader.dbLoad(ruleMap);
            }catch (Exception e){
                e.printStackTrace();
            }


        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取一个计数器用来记录不合法的日志行数，组名，计算器名称
            Counter counter = context.getCounter("malformed", "malformedline");
            String line = value.toString();
            String[] fields= StringUtils.split(line, "\t");
            try{
                String url=fields[26];
                String content_tag=ruleMap.get(url);
                if(content_tag==null){
                    key.set(Long.parseLong(url+"\t"+"tocrawl"+"\n"));
                    context.write(k,v);
                }else{
                    k.set(line+"\t"+content_tag+"\n");
                    context.write(k,v);
                }

            }catch (Exception e){
                counter.increment(1);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf=new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(LogEnhance.class);
        job.setMapperClass(LogEnhanceMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //要控制不同的内容写往不同的目标路径，可以采用自定义outputformat的方法
        job.setOutputFormatClass(LogEnhanceOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("E:/wordcount/webloginput/"));
        FileOutputFormat.setOutputPath(job,new Path("E:/temp/output/"));

        //不需要reducer
        job.setNumReduceTasks(0);

        job.waitForCompletion(true);

        System.exit(0);

    }
}
