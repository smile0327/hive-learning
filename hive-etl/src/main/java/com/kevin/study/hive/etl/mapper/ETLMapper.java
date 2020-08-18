package com.kevin.study.hive.etl.mapper;

import com.kevin.study.hive.etl.util.ETLUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: kevin
 * @Description:  数据只是做简单的ETL，所以不需要reduce阶段，直接清洗好之后写入文件即可
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 16:22 2020/6/30
 * @ProjectName: hive-learning
 */
public class ETLMapper extends Mapper<LongWritable,Text,NullWritable,Text> {

    //全局的
    Text t = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、获取原始数据
        String line = value.toString();
        //2、过滤数据
        String etrStr = ETLUtil.oriString2ETLString(line);
        if (etrStr == null){
            return;
        }
        //3、写出数据
        t.set(etrStr);
        context.write(NullWritable.get() , t);

    }

}
