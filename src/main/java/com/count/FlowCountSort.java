package com.count;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by Administrator on 2018/2/26.
 */
public class FlowCountSort {
    private static Logger logger= LogManager.getLogger("FlowCount");
    static class FlowCountSortMapper extends Mapper<LongWritable,Text,FlowBean,Text>{


        FlowBean bean=new FlowBean();
        Text v=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line=value.toString();

            String[] fields = line.split("\t");

            String phoneNbr = fields[0];

            long upFlow = Long.parseLong(fields[1]);
            long dFlow = Long.parseLong(fields[2]);

            bean.set(upFlow,dFlow);
            v.set(phoneNbr);

            context.write(bean,v);
        }
    }

    static class FlowCountSortReducer extends Reducer<FlowBean,Text,Text,FlowBean>{

        @Override
        protected void reduce(FlowBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            context.write(values.iterator().next(),bean);
        }
    }

    public static void main(String[] args) {
        logger.info("FlowCount开始运作");
        Configuration conf = new Configuration();
        conf.addResource(Resources.getResource("core-site.xml"));

        try {
            Job job = Job.getInstance(conf, "FlowCount");
            //指定本程序的jar包的所在的本地路径
            job.setJarByClass(FlowCountSort.class);
            //指定本业务job要使用的mapper/Reducer业务类
            job.setMapperClass(FlowCountSortMapper.class);
            job.setReducerClass(FlowCountSortReducer.class);

            //指定map输出数据的kv类型
            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);
            //指定最终输出的数据kv类
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);
            //指定job输入的原始文件所在目录
            FileInputFormat.addInputPath(job,new Path("/FlowCountSort/input"));
            //指定job的输出结果所在目录
            FileOutputFormat.setOutputPath(job,new Path("/FlowCountSort/output"));
            //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn运行
            boolean flag = job.waitForCompletion(true);
            logger.info("FlowCountSort结束运作，运行结果："+flag);
        } catch (IOException|InterruptedException|ClassNotFoundException e) {
            logger.error("FlowCountSort结束失败");
            logger.error("FlowCountSort.main",e);
        }


    }

}
