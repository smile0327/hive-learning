package com.kevin.study.hive.function.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * @Auther: kevin
 * @Description:
 *
 * 继承GenericUDF类，除了可以处理基本数据类型，还可以处理复杂数据类型（Map,List,Struct等）
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 9:43 2020/6/30
 * @ProjectName: hive-learning
 */
public class MySplit extends GenericUDF {

    /**
     * 这个方法只调用一次，并且在evaluate()方法之前调用。
     * 该方法接受的参数是一个ObjectInspectors数组。
     * 该方法检查接受正确的参数类型和参数个数。
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        return null;
    }

    /**
     * 这个方法类似UDF的evaluate()方法。它处理真实的参数，并返回最终结果。
     * @param deferredObjects
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        return null;
    }

    /**
     * //这个方法用于当实现的GenericUDF出错的时候，打印出提示信息。而提示信息就是你实现该方法最后返回的字符串。
     * @param strings
     * @return
     */
    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }

}
