package wuxian.me.hiveudf.d;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

import java.util.ArrayList;

@UDFType(deterministic = true)
@Description(name = "wrapper",
        value = "_FUNC_() ")
@NDV(maxNdv = 1)
public class WrapperUDF extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        System.out.println("initialize objectInspectors[] ");
        int i = 0;
        for (ObjectInspector objectInspector : objectInspectors) {
            System.out.print("objectInspector" + i + ": " + objectInspector + ",    ");
            i++;
        }
        System.out.println();

        ArrayList<String> fieldNames = new ArrayList<String>();
        fieldNames.add("firstInteger");
        fieldNames.add("secondString");
        ArrayList<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>();
        fieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);


        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
        //return ObjectInspectorFactory.getStandardUnionObjectInspector(Arrays.<ObjectInspector>asList(PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector));

        //return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldObjectInspectors);

        //return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        System.out.println("evaluate deferredObjects[]");
        int i = 0;
        for (DeferredObject objectInspector : deferredObjects) {
            System.out.print("deferredObject" + i + ": " + objectInspector + ",    ");
            i++;
        }
        System.out.println();

        ArrayList<Object> struct = new ArrayList<Object>(3);
        struct.add(1);

        struct.add("two");

        StandardUnionObjectInspector.StandardUnion union = new StandardUnionObjectInspector.StandardUnion((byte) 0, "foo");
        //union.
        return new BooleanWritable(true);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "WRAPPER";
    }
}
