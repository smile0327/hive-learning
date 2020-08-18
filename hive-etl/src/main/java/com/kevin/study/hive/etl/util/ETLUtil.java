package com.kevin.study.hive.etl.util;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 16:27 2020/6/30
 * @ProjectName: hive-learning
 */
public class ETLUtil {

    public static String oriString2ETLString(String ori) {
        StringBuilder etlString = new StringBuilder();
        String[] splits = ori.split("\t");
        if (splits.length < 9) {
            return null;
        }
        splits[3] = splits[3].replace(" ", "");
        for (int i = 0; i < splits.length; i++) {
            if (i < 9) {
                if (i == splits.length - 1) {
                    etlString.append(splits[i]);
                } else {
                    etlString.append(splits[i] + "\t");
                }
            } else {
                if (i == splits.length - 1) {
                    etlString.append(splits[i]);
                } else {
                    etlString.append(splits[i] + "&");
                }
            }
        }
        return etlString.toString();
    }
}
