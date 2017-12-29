package wuxian.me.datetimesparksql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import wuxian.me.datetimesparksql.util.DebugUtil;
import wuxian.me.datetimesparksql.util.ImportPostgresUtil;
import wuxian.me.datetimesparksql.util.MetastoreConf;
import wuxian.me.datetimesparksql.util.PgJdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Properties;

@UDFType(deterministic = true)
@Description(name = "debug",
        value = "_FUNC_() ")
@NDV(maxNdv = 1)
public class DebugUDF extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        new DebugUtil(SessionState.get()).debug(objectInspectors);
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        return new BooleanWritable(true);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "DEBUG";
    }
}
