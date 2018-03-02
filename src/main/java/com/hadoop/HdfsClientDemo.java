package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

/**
 * 客户端操作hdfs时候，是有一个身份的问题
 * 在构造客户端fs对象时，通过参数传递进去
 *
 * Created by Administrator on 2018/2/24.
 */
public class HdfsClientDemo {
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
    //上传
    @Test
    public void testUpload() throws IOException {

        fs.copyFromLocalFile(new Path("c:/client.jar"),new Path("/client.jar"));
        fs.close();
    }

    //下载
    @Test
    public void testDownload() throws IOException {

        fs.copyToLocalFile(new Path("/wordcount/input"),new Path("c:/input.txt"));
        fs.close();
    }

    //打印参数
    @Test
    public void testConf(){

        Iterator<Map.Entry<String, String>> it = conf.iterator();
        while(it.hasNext()){
            Map.Entry<String, String> ent = it.next();
            System.out.println(ent.getKey()+":"+ent.getValue());
        }

    }

    @Test
    public void testMkdir() throws IOException {

        boolean mkdirs = fs.mkdirs(new Path("/testMkdir"));
        System.out.println(mkdirs);
    }

    @Test
    public void testDelete() throws IOException {

        boolean delete = fs.delete(new Path("/testmkdir/aaa"), true);
        System.out.println(delete);
    }

    /**
     *递归列出指定目录下所有子文件夹中的文件
     * @throws IOException
     */
    @Test
    public void testLs() throws IOException {

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("blocksize:"+fileStatus.getBlockLocations());
            System.out.println("owner:"+fileStatus.getOwner());
            System.out.println("Replication:"+fileStatus.getReplication());
            System.out.println("Permission:"+fileStatus.getPermission());
            System.out.println("name:"+fileStatus.getPath().getName());
            System.out.println("----------------------------------------");

        }
    }

    @Test
    public void testLs2() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus file:fileStatuses) {

            System.out.println("name:"+file.getPath().getName());
            System.out.println((file.isFile()?"file":"directory"));

        }
    }
}
