package wuxian.me.datetimesparksql;

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
import wuxian.me.datetimesparksql.util.PgJdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Properties;

@UDFType(deterministic = true)
@Description(name = "import_postgres",
        value = "_FUNC_() import data from postgres.usage select import_postgres('url=xxx username=xxx password=xxx'" +
                ",'insert into aaa select * from pg.bbb')")
@NDV(maxNdv = 1)
public class ImportPostgresUDF extends GenericUDF {

    private static boolean debug = true;

    private String executeResult;
    private String url;
    private String username;
    private String password;
    private String sql;
    private boolean ret;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (debug) {
            new DebugUtil(SessionState.get()).debug();
            return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
        }

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

            System.out.println("PARAM url: " + url + " username: " + username + " password:" + password);
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

            System.out.println("PARAM sql: " + sql);
        }

        System.out.println("try connect to postgres");
        Connection connection = null;
        try {
            PgJdbc.initDriver();
            connection = PgJdbc.getConnectionBy(this.url, this.username, this.password);
        } catch (Exception e) {
            throw new UDFArgumentException("can't connect to postgresql");
        }
        System.out.println("connect to postgres success");

        System.out.println("try init hive driver");
        SessionState state = SessionState.get();
        Driver driver = new Driver(state.getConf());
        System.out.println("init hive driver success");

        if (!ImportPostgresUtil.isValidInsertSelectSQL(sql)) {
            throw new UDFArgumentException("sql not valid!");
        }

        System.out.println("sql is a valid insert-into-select sql");

        String selectSQL = ImportPostgresUtil.getPGSelectSQL(sql);
        ResultSet resultSet = null;
        try {
            resultSet = ImportPostgresUtil.getSelectPGResult(selectSQL, connection);
        } catch (Exception e) {

        } finally {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }

        try {
            System.out.println("selectClauser: " + selectSQL + " return resultSet size: " + resultSet.getFetchSize());
        } catch (Exception e) {

        }

        String insertSQL = ImportPostgresUtil.getHiveInsertSQL(sql);

        try {
            ret = ImportPostgresUtil.insertHiveTableBy(driver, insertSQL, resultSet);
        } catch (Exception e) {

        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {

                }
            }
        }
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }


    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        return new BooleanWritable(ret);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "IMPORT_POSTGERS";
    }
}
