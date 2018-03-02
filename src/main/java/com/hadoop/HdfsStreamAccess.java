package com.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2018/2/24.
 */
public class HdfsStreamAccess {

    FileSystem fs=null;
    Configuration conf=null;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        conf = new Configuration();
        //告诉这里用的是什么文件系统
        conf.set("fs.defaultFS","hdfs://master1.hadoop:9000");
        //拿到一个文件系统操作的客户端实例对象,
        //fs=FileSystem.get(conf);
        //可以直接拿到用户身份，这样就不存在权限的问题
        fs = FileSystem.get(new URI("hdfs://master1.hadoop:9000"),conf,"hadoop");
    }

    /**
     *通过流的方式获取上传文件到hdfs
     */
    @Test
    public void testUpload() throws IOException {

        FSDataOutputStream outputStream = fs.create(new Path("/angelababy.love"), true);
        //从本地拿个输入流
        FileInputStream inputStream = new FileInputStream("c:/angelababy.love");
        //这里是封装工具
        IOUtils.copy(inputStream,outputStream);

    }

    /**
     *通过流的方式将文件写入到本地磁盘
     * @throws IOException
     */
    @Test
    public void testDownLoad() throws IOException {

        FSDataInputStream inputStream = fs.open(new Path("/angelababy.love"));

        FileOutputStream outputStream = new FileOutputStream("d:/angelababy.love");

        IOUtils.copy(inputStream,outputStream);

    }

    /**
     * 通过流的方式指定读取任务的位置
     *
     * @throws IOException
     */
    @Test
    public void testRandomAccess() throws IOException {

        FSDataInputStream inputStream=fs.open(new Path("/angelababy.love"));
        //流里面可以定义从哪里开始读取数据
        inputStream.seek(12);

        FileOutputStream outputStream = new FileOutputStream("d:/angelababy.love");

        IOUtils.copy(inputStream,outputStream);
    }

    /**
     *显示hdfs上文件的内容
     * @throws IOException
     */
    @Test
    public void testCat() throws IOException {

        FSDataInputStream inputStream = fs.open(new Path("/angelababy.love"));

        IOUtils.copy(inputStream,System.out);
    }

}
