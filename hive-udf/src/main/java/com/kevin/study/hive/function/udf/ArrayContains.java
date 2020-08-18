package com.kevin.study.hive.function.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.List;

/**
 * @Auther: kevin
 * @Description: 继承GenericUDF类，除了可以处理基本数据类型，还可以处理复杂数据类型（Map,List,Struct等）
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 9:43 2020/6/30
 * @ProjectName: hive-learning
 */
@Description(name = "arrayContains", value = "param1:array,param2:value, if array contains value return true")
public class ArrayContains extends GenericUDF {

    ListObjectInspector listOI;
    StringObjectInspector elementOI;


    /**
     * 只调用一次，并且在evaluate()方法之前调用。
     * 接受的参数是一个ObjectInspectors数组。
     * 检查接受正确的参数类型和参数个数。
     * 初始化的时候主要做一些校验工作
     * @param objectInspectors 调用该函数传入的参数
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
        }
        // 1、检查参数的类型
        ObjectInspector a = objectInspectors[0];
        ObjectInspector b = objectInspectors[1];
        if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
        }
        this.listOI = (ListObjectInspector) a;
        this.elementOI = (StringObjectInspector) b;

        //2、检查list中的元素是否都是String
        if(!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list of strings");
        }

        // 返回类型是boolean，所以我们提供了正确的object inspector
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    /**
     * 类似UDF的evaluate()方法。它处理真实的参数，并返回最终结果。
     *
     * @param deferredObjects  参数
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        // 利用object inspectors从传递的对象中得到list与string
        List<String> list = (List<String>) listOI.getList(deferredObjects[0].get());
        String arg = elementOI.getPrimitiveJavaObject(deferredObjects[1].get());
        // 检查空值
        if (list == null || arg == null) {
            return null;
        }

        // 判断是否list中包含目标值
        for(String s: list) {
            if (arg.equals(s)) return new Boolean(true);
        }
        return new Boolean(false);
    }

    /**
     * 当实现的GenericUDF出错的时候，打印出提示信息。而提示信息就是你实现该方法最后返回的字符串。
     *
     * @param strings
     * @return
     */
    @Override
    public String getDisplayString(String[] strings) {
        return "eg:arrayContains(array(1,2),1)";
    }

}
