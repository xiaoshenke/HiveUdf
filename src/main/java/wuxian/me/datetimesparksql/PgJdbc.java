package wuxian.me.datetimesparksql;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.sql.*;
import java.util.*;

public class PgJdbc {

    private static final String DRIVER = "org.postgresql.Driver";
    private static boolean inited = false;

    public static void initDriver() throws HiveException {
        if (inited) {
            return;
        }
        try {
            Class.forName(DRIVER);
            inited = true;
        } catch (Exception e) {
            System.out.println("exception: " + e.getMessage());
            e.printStackTrace();
            throw new HiveException("postgres driver init error");
        }
    }

    public static Map<String, Connection> connectionMap = new HashMap<>();

    public static Connection getConnectionBy(String url, String username, String password) throws HiveException {
        if ((url == null || url.length() == 0) && (username == null || username.length() == 0) && (password == null || password.length() == 0)) {
            throw new HiveException("url && username && password is empty");
        }

        String key = url + username + password;
        if (connectionMap.containsKey(key)) {
            try {
                if (!connectionMap.get(key).isClosed()) {
                    return connectionMap.get(key);
                }
            } catch (Exception e) {
                ;
            }
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, username, password);
            connectionMap.put(key, conn);
            return conn;
        } catch (Exception e) {

        }
        return conn;
    }


    public static void safelyClose(ResultSet resultSet, PreparedStatement pstmt) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {

                e.printStackTrace();
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}