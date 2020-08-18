package com.kevin.study.hive.function.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Auther: kevin
 * @Description: 自定义UDF 函数
 *  1、UDF  一进一出
 *  2、UDAF  多进一出，聚合函数
 *  3、UDTF  一进多出，炸裂函数
 *
 *  继承至UDF类，只能处理基本类型的数据
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 15:45 2020/6/29
 * @ProjectName: hive-learning
 */
//对方法描述  name：函数名  value：函数参数  extended：额外说明，示例等
@Description(name = "mylower",value = "字符串",extended = "mylower(name)")
public class MyLower extends UDF {

    /**
     * 自定义函数  转小写
     * 自定义函数编程步骤：
     * 1、继承UDF类
     * 2、实现evaluate函数；evaluate函数支持重载
     * 3、在hive命令行窗口创建函数
     *   3.1、添加jar
     *   add jar jar_path
     *   3.2、创建function，并与开发好的java class进行关联
     *   create [temporary] function [if exists] [dbname.]function_name as "MyLower";
     *
     *  UDF函数必须有返回类型，可以返回NULL，但是返回类型不能为void
     * @param s
     * @return
     */
    public String evaluate(final String s){
        return s == null ? null : s.toLowerCase();
    }

}
