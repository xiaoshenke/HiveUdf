package wuxian.me.datetimesparksql.d;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

@UDFType(deterministic = true)
@Description(name = "get_sparksession",
        value = "_FUNC_() - Returns the current date at the start of query evaluation."
                + " All calls of current_date within the same query return the same value.")
@NDV(maxNdv = 1)
public class GetSparkSessionUDF extends GenericUDF {

    public ObjectInspector initialize(ObjectInspector[] objectInspectors)
            throws UDFArgumentException {

        if (objectInspectors.length != 0) {
            throw new UDFArgumentLengthException("no argment is accepted!");
        }
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        HiveConf conf = SessionState.get().getConf();
        SparkSessionManager sparkSessionManager = SparkSessionManagerImpl.getInstance();

        SparkSession ss = SparkUtilities.getSparkSession(conf, sparkSessionManager);

        return ss == null ? new Boolean(false) : new Boolean(true);//Todo: not working
    }

    public String getDisplayString(String[] strings) {
        return "GET_SPARKSESSION()";
    }
}
