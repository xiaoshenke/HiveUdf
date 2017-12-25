package wuxian.me.datetimesparksql;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

//ref:http://blog.csdn.net/xiao_jun_0820/article/details/53404939

@UDFType(deterministic = true)
@Description(name = "export_postgres",
        value = "_FUNC_() export data to postgres.")
@NDV(maxNdv = 1)
public class ExportPostgresUDF extends GenericUDF {

    private String sql;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length < 3) {
            throw new UDFArgumentException(" Expecting  at least three  arguments ");
        }

        if (objectInspectors[0].getCategory() == ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(objectInspectors[0] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("postgres connection url must be constant");
            }
            ConstantObjectInspector propertiesPath = (ConstantObjectInspector) objectInspectors[0];
            String pgurl = propertiesPath.getWritableConstantValue().toString();

            Properties properties = PgJdbc.parsePostGresUrl(pgurl);

            this.user = properties.getProperty("username");
            this.url = properties.getProperty("url");
            this.password = properties.getProperty("password");
            properties.setProperty("maxActive", String.valueOf(20));

            try {
                this.dataSource = BasicDataSourceFactory.createDataSource(properties);
            } catch (Exception e) {
                throw new UDFArgumentException("create postgres connection fail!");
            }

        }

        if (objectInspectors[1].getCategory() == ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) objectInspectors[1]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(objectInspectors[1] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("the second arg must be a sql string constant");
            }
            ConstantObjectInspector sqlInsp = (ConstantObjectInspector) objectInspectors[1];
            this.sql = sqlInsp.getWritableConstantValue().toString();
            if (this.sql == null || this.sql.trim().length() == 0) {
                throw new UDFArgumentException("the second arg must be a sql string constant and not nullable");
            }
        }
        paramsInspectors = new PrimitiveObjectInspector[objectInspectors.length - 2];
        for (int i = 2; i < objectInspectors.length; i++) {
            paramsInspectors[i - 2] = (PrimitiveObjectInspector) objectInspectors[i];
        }

        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    private String url;
    private String user;
    private String password;

    private DataSource dataSource;

    private PrimitiveObjectInspector[] paramsInspectors;

    public void close() throws IOException {
        try {
            BasicDataSource bds = (BasicDataSource) dataSource;
            bds.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }


    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }


    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            System.out.println("execute sql:" + System.currentTimeMillis());
            for (int i = 2; i < deferredObjects.length; i++) {
                Object param = paramsInspectors[i - 2].getPrimitiveJavaObject(deferredObjects[i].get());
                stmt.setObject(i - 1, param);
            }
            int ret = stmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new HiveException(e);
        }

        return new BooleanWritable(true);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "EXPORT_POSTGRES";
    }
}
