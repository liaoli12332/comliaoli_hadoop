package com.partitioner;


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
public class FlowCount {
    private static Logger logger= LogManager.getLogger("FlowCount");
    static class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将内容转化成String
            String line = value.toString();
            //按tab切分字段
            String[] fields = line.split("\t");
            //取出手机号
            String phoneNbr = fields[1];
            //取出上行流量和下行流量
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long dFlow = Long.parseLong(fields[fields.length - 2]);
            context.write(new Text(phoneNbr),new FlowBean(upFlow,dFlow));

        }
    }

    static class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean>{

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            long sum_upFlow=0;
            long sum_dFlow=0;

            //遍历所有bean，将其中的上行流量和下行流量分别累加
            for (FlowBean bean:values) {
                sum_upFlow+=bean.getUpFlow();
                sum_dFlow+=bean.getdFlow();
            }

            FlowBean resultBean = new FlowBean(sum_upFlow, sum_dFlow);
            context.write(key,resultBean);
        }
    }

    public static void main(String[] args) {
        logger.info("FlowCount开始运作");
        Configuration conf = new Configuration();
        conf.addResource(Resources.getResource("core-site.xml"));

        try {
            Job job = Job.getInstance(conf, "FlowCount");
            //指定本程序的jar包的所在的本地路径
            job.setJarByClass(FlowCount.class);
            //指定本业务job要使用的mapper/Reducer业务类
            job.setMapperClass(FlowCountMapper.class);
            job.setReducerClass(FlowCountReducer.class);
            //指定map输出数据的kv类型
            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);
            //指定我们自定义的数据分区器
            job.setPartitionerClass(ProvincePartitioner.class);
            //同时指定相应分区数量的reducetask，这里是有5个分区
            job.setNumReduceTasks(5);
            //指定最终输出的数据kv类
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);
            //指定job输入的原始文件所在目录
            FileInputFormat.addInputPath(job,new Path("/FlowCount/input"));
            //指定job的输出结果所在目录
            FileOutputFormat.setOutputPath(job,new Path("/FlowCount/output"));
            //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn运行
            boolean flag = job.waitForCompletion(true);
            logger.info("FlowCount结束运作，运行结果："+flag);
        } catch (IOException|InterruptedException|ClassNotFoundException e) {
            logger.error("FlowCountJob结束失败");
            logger.error("FlowCount.main",e);
        }


    }
}
