package wuxian.me.datetimesparksql;

import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import java.util.Properties;

@UDFType(deterministic = true)
@Description(name = "import_postgres",
        value = "_FUNC_() import data from postgres.usage select import_postgres('url=xxx username=xxx password=xxx'" +
                ",'insert into aaa select * from pg.bbb')")
@NDV(maxNdv = 1)
public class ImportPostgresUDF extends GenericUDF {

    private String executeResult;
    private String url;
    private String username;
    private String password;
    private String sql;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {


        if (objectInspectors.length < 2) {
            throw new UDFArgumentException(" Expecting  at least two arguments ");
        }

        if (objectInspectors[0].getCategory() == ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(objectInspectors[0] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("postgres connection url must be constant");
            }
            ConstantObjectInspector propertiesPath = (ConstantObjectInspector) objectInspectors[0];
            String pgurl = propertiesPath.getWritableConstantValue().toString();

            Properties properties = PgJdbc.parsePostGresUrl(pgurl);

            this.url = properties.getProperty("url");
            this.username = properties.getProperty("username");
            this.password = properties.getProperty("password");
        }

        if (objectInspectors[1].getCategory() == ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) objectInspectors[1]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(objectInspectors[1] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("second arg must be a sql string constant");
            }
            ConstantObjectInspector sqlInsp = (ConstantObjectInspector) objectInspectors[1];
            this.sql = sqlInsp.getWritableConstantValue().toString();
            if (this.sql == null || this.sql.trim().length() == 0) {
                throw new UDFArgumentException("second arg must be a sql string constant and not nullable");
            }
        }

        /*
        SessionState state = SessionState.get();
        Driver driver = new Driver(state.getConf());
        CommandProcessorResponse response = null;
        String sql = "show databases";
        try {
            response = driver.run(sql);
        } catch (Exception e) {

            throw new UDFArgumentException("execute sql: " + sql + " error!");
        }
        executeResult = response.toString();
        */
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    //Todo
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        return new BooleanWritable(true);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "IMPORT_POSTGERS";
    }
}
