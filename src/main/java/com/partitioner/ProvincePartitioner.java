package com.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Created by Administrator on 2018/2/26.
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    //这里将数据缓存在内存中，这样的话，可以高效的读取数据
    public static HashMap<String,Integer> provinceDict=new HashMap<>();
    static{
        provinceDict.put("136",0);
        provinceDict.put("137",1);
        provinceDict.put("138",2);
        provinceDict.put("139",3);
    }
    /**
     *
     *k2 v2 对应是map输出kv的类型
     * @param key
     * @param flowBean
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text key, FlowBean flowBean, int i) {
        String prefix=key.toString().substring(0,3);
        Integer provinceId = provinceDict.get(prefix);
        return provinceId==null?4:provinceId;
    }
}
