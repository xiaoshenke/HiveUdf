package wuxian.me.datetimesparksql;

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
@Description(name = "import_postgres",
        value = "_FUNC_() import data from postgres.usage select import_postgres('url=xxx username=xxx password=xxx'" +
                ",'insert into aaa select * from pg.bbb')")
@NDV(maxNdv = 1)
public class ImportPostgresUDF extends GenericUDF {

    private String sql;
    private boolean ret;

    private Connection connection;

    private void initPostgresConnection(ObjectInspector objectInspector) throws UDFArgumentException {

        if (objectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(objectInspector instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("postgres connection url must be constant");
            }
            ConstantObjectInspector propertiesPath = (ConstantObjectInspector) objectInspector;
            String pgurl = propertiesPath.getWritableConstantValue().toString();

            Properties properties = PgJdbc.parsePostGresUrl(pgurl);
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");

            System.out.println("try connect to postgres");
            try {
                PgJdbc.initDriver();
                connection = PgJdbc.getConnectionBy(url, username, password);
            } catch (Exception e) {
                throw new UDFArgumentException("can't connect to postgresql");
            }
            System.out.println("connect to postgres success");
        } else {
            throw new UDFArgumentException("not valid connection url");
        }
    }

    private String database;
    private String schema;

    //insert into hive.xxx select * from pg.yyy;
    private void getInsertSelectString(ObjectInspector objectInspector) throws UDFArgumentException {

        if (objectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(objectInspector instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("arg must be a sql string constant");
            }
            ConstantObjectInspector sqlInsp = (ConstantObjectInspector) objectInspector;
            String originSql = sqlInsp.getWritableConstantValue().toString();
            if (originSql == null || originSql.trim().length() == 0) {
                throw new UDFArgumentException("arg must be a sql string constant and not nullable");
            }

            String insertIntoSql = ImportPostgresUtil.getInsertIntoString(originSql);
            if (insertIntoSql == null) {
                throw new UDFArgumentException("invalid sql string!");
            }

            String databaseDot = ImportPostgresUtil.getDatabaseDot(insertIntoSql);
            if (databaseDot == null) {
                throw new UDFArgumentException("invalid sql string!");
            }

            String schemaDot = ImportPostgresUtil.getSchemaDot(originSql);
            if (schemaDot != null) {
                this.schema = schemaDot.substring(0, schemaDot.length() - 1);
            }

            int index = originSql.indexOf(databaseDot);
            StringBuilder builder = new StringBuilder("");
            builder.append(originSql.substring(0, index));

            if (schemaDot != null) {
                int schIndex = originSql.indexOf(schemaDot);
                builder.append(originSql.substring(index + databaseDot.length(), schIndex));
                builder.append(originSql.substring(schIndex + schemaDot.length()));
            } else {
                builder.append(originSql.substring(index + databaseDot.length()));
            }

            this.sql = builder.toString();
            String hiveDatabase = databaseDot.substring(0, databaseDot.length() - 1);
            this.database = hiveDatabase;

            System.out.println("hiveDb: " + hiveDatabase + " ,formatted sql: " + sql);

        } else {
            throw new UDFArgumentException("not a valid insert-select sql string");
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length < 2) {
            throw new UDFArgumentException(" Expecting  at least two arguments ");
        }

        //1 get Insert-Into-Select sql
        getInsertSelectString(objectInspectors[1]);

        //2 check sql
        if (false && !ImportPostgresUtil.isValidInsertSelectSQL(sql)) {
            throw new UDFArgumentException("sql not valid!");
        }

        //3 check postgres connection
        initPostgresConnection(objectInspectors[0]);

        //4 import postgres data
        String selectSQL = ImportPostgresUtil.getPGSelectSQL(sql, schema);
        System.out.println("pg select sql: " + selectSQL);
        ResultSet resultSet = null;
        try {
            resultSet = ImportPostgresUtil.getSelectPGResult(selectSQL, connection);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception: " + e.getMessage());

        } finally {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e) {
            }
        }

        //5 insert to hive
        String insertSQL = ImportPostgresUtil.getHiveInsertSQL(sql, true, database);
        try {
            ret = ImportPostgresUtil.insertHiveTableBy(insertSQL, resultSet, database);
        } catch (Exception e) {
            e.printStackTrace();
            ret = false;
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
