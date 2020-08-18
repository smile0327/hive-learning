package com.kevin.study.hive.etl;

import com.kevin.study.hive.etl.mapper.ETLMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 8:38 2020/7/1
 * @ProjectName: hive-learning
 */
public class ETLDriver implements Tool {

    private Configuration _configuration;

    public static void main(String[] args) {
        Configuration config = new Configuration();
        try {
            int run = ToolRunner.run(config, new ETLDriver(), args);
            System.out.println(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        _configuration = this.getConf();
        _configuration.set("inpath" , args[0]);
        _configuration.set("outpath" , args[1]);

        //1、获取Job对象
        Job job = Job.getInstance(_configuration , "video-etl");
        //2、设置Jar包路径
        job.setJarByClass(ETLDriver.class);
        //3、设置Mapper类&输出KV类型
        job.setMapperClass(ETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        //4、设置最终输出的KV类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //5、设置输入输出的路径
        initJobInputPath(job);
        initJobOutputPath(job);
        //6、提交任务
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 根据参数 初始化输入路径
     * @param job
     * @throws IOException
     */
    private void initJobInputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String inpath = conf.get("inpath");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(inpath);
        if (fs.exists(path)){
            FileInputFormat.addInputPath(job , path);
        }else {
            throw  new RuntimeException("HDFS中该文件目录不存在:" + inpath);
        }
    }

    /**
     * 根据参数初始化输出路径
     * @param job
     * @throws IOException
     */
    private void initJobOutputPath(Job job) throws IOException{
        Configuration conf = job.getConfiguration();
        String outpath = conf.get("outpath");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outpath);
        if (fs.exists(path)) {
            fs.delete(path  ,true);
        }
        FileOutputFormat.setOutputPath(job , path);
    }

    @Override
    public void setConf(Configuration configuration) {
        _configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return _configuration;
    }

}
