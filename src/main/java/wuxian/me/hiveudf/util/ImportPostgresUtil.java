package wuxian.me.hiveudf.util;

import com.sun.istack.Nullable;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.getQualifiedTableName;

public class ImportPostgresUtil {

    private ImportPostgresUtil() {
    }

    //Fixme:
    public static boolean isValidInsertSelectSQL(String sql) {
        try {
            ASTNode tree = ParseTreeUtil.genASTNodeFrom(sql);
            if (tree == null) {
                return false;
            }
            ASTNode queryNode = (ASTNode) tree.getChild(0);
            if (queryNode.getToken().getType() != HiveParser.TOK_QUERY) {
                throw new UDFArgumentException("not a valid query sql!");
            }
            if (queryNode.getChildCount() != 2) {
                throw new UDFArgumentException("not a valid query sql!");
            }
            ASTNode fromNode = (ASTNode) queryNode.getChild(0);
            if (fromNode.getToken().getType() != HiveParser.TOK_FROM) {
                throw new UDFArgumentException("not a valid query sql!");
            }
            ASTNode insertNode = (ASTNode) queryNode.getChild(1);
            if (insertNode.getToken().getType() != HiveParser.TOK_INSERT) {
                throw new UDFArgumentException("not a valid query sql!");
            }
            ASTNode insertIntoNode = (ASTNode) insertNode.getChild(0);
            if (insertIntoNode.getToken().getType() != HiveParser.TOK_INSERT_INTO) {
                throw new UDFArgumentException("not a valid query sql!");
            }
            if (insertNode.getFirstChildWithType(HiveParser.TOK_SELECT) == null) {
                throw new UDFArgumentException("not a valid query sql!");
            }
        } catch (UDFArgumentException e) {
            return false;
        }
        return true;
    }

    // insert into xxx values (),(),(),();
    public static String combineSql(String insertSql, ResultSetMetaData metaData, ResultSet resultSet) throws SQLException {
        StringBuilder builder = new StringBuilder(insertSql + " values");
        if (resultSet.next()) {
            while (true) {
                builder.append("(");
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    //System.out.println("columnClassName: " + metaData.getColumnClassName(i));
                    if (metaData.getColumnClassName(i).equalsIgnoreCase("java.lang.String")) {
                        builder.append("'" + resultSet.getString(i) + "'");
                    } else {
                        builder.append(resultSet.getString(i));  //string is safe
                    }
                    if (i + 1 <= metaData.getColumnCount()) {
                        builder.append(",");
                    }
                }
                builder.append(")");
                if (!resultSet.next()) {
                    break;
                }
                builder.append(",");
            }
        }
        builder.append(";");
        return builder.toString();
    }

    private static String tmpPath = FileUtil.getCurrentPath() + "/tmp.sql";

    public static boolean insertHiveTableBy(String insertSql, ResultSet resultSet, String database) throws Exception {
        if (insertSql == null || insertSql.length() == 0 || resultSet == null) {
            return false;
        }

        ResultSetMetaData metaData = resultSet.getMetaData();
        String sql = combineSql(insertSql, metaData, resultSet);
        System.out.println("combined sql: " + sql);
        FileUtil.writeToFile(tmpPath, sql);

        ProcessBuilder processBuilder = null;
        processBuilder = new ProcessBuilder("hive", "-f", tmpPath);
        Process p = processBuilder.start();
        p.waitFor();
        final InputStream is = p.getInputStream();
        final java.io.BufferedReader reader =
                new java.io.BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        is.close();
        return true;
    }


    public static String getTableNameFromSQL(ASTNode tree, String sql) throws UDFArgumentException {
        if (sql == null || sql.length() == 0) {
            throw new UDFArgumentException("not a valid query sql!");
        }
        ASTNode fromNode = (ASTNode) tree.getFirstChildWithType(HiveParser.TOK_FROM);
        if (fromNode == null) {
            throw new UDFArgumentException("can't find table name!");
        }

        ASTNode table = (ASTNode) fromNode.getFirstChildWithType(HiveParser.TOK_TABNAME);
        if (table == null) {
            throw new UDFArgumentException("can't find table name!");
        }
        try {
            String[] names = getQualifiedTableName(table);
            StringBuilder ret = new StringBuilder("");
            for (String n : names) {
                ret.append(n);
            }
            return ret.toString();

        } catch (Exception e) {
            throw new UDFArgumentException("can't find table name!");
        }
    }

    private static final String INSERT_INTO_REG = "insert\\s*into\\s*[a-z0-9A-Z]*[.][a-z0-9A-Z]*";
    private static final Pattern INSERT_INTO_PATTERN = Pattern.compile(INSERT_INTO_REG);

    private static final String INSERT_INTO_REG_3 = "into\\s*";
    private static final Pattern INSERT_INTO_PATTERN_3 = Pattern.compile(INSERT_INTO_REG_3);


    private static final String DATABASE_REG = "[a-z0-9A-Z]*[.]";
    private static final Pattern DATABASE_PATTERN = Pattern.compile(DATABASE_REG);

    private static final String SCHEMA_REG = "from\\s*[a-z0-9A-Z]*[.][a-z0-9A-Z]*";
    private static final Pattern SCHEMA_PATTERN = Pattern.compile(SCHEMA_REG);
    private static final String SCHEMA_REG_2 = "[a-z0-9A-Z]*[.]";
    private static final Pattern SCHEMA_PATTERN_2 = Pattern.compile(SCHEMA_REG_2);

    private static final String SCHEMA_REG_3 = "from\\s*";
    private static final Pattern SCHEMA_PATTERN_3 = Pattern.compile(SCHEMA_REG_3);

    @Nullable
    public static String getInsertIntoString(String origin) {
        Matcher matcher = INSERT_INTO_PATTERN.matcher(origin.toLowerCase());
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

    @Nullable
    //返回null说明是没有schema的 for Postgres.Schema
    public static String getSchemaDot(String origin) {
        Matcher matcher = SCHEMA_PATTERN.matcher(origin.toLowerCase());
        if (matcher.find()) {
            String schema = matcher.group();
            Matcher matcher1 = SCHEMA_PATTERN_2.matcher(schema);
            if (matcher1.find()) {
                return matcher1.group();
            }
        }
        return null;
    }

    public static String getDatabaseDot(String origin) {
        Matcher matcher = DATABASE_PATTERN.matcher(origin);
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }


    public static ResultSet getSelectPGResult(String selectSQL, Connection connection) throws UDFArgumentException, SQLException {
        PreparedStatement pstmt = connection.prepareStatement(selectSQL);
        ResultSet resultSet = pstmt.executeQuery();
        if (pstmt != null) {
            //pstmt.close();
        }
        return resultSet;
    }

    public static String getPGSelectSQL(String sql, @Nullable String schema) throws UDFArgumentException {
        if (sql == null || sql.length() == 0) {
            throw new UDFArgumentException("not a valid query sql!");
        }
        int selectIndex = sql.toLowerCase().indexOf("select");  //注意大小写
        if (selectIndex == -1) {
            throw new UDFArgumentException("not a valid query sql!");
        }
        String selectSql = sql.substring(selectIndex, sql.length());

        if (schema != null && schema.length() != 0) {
            Matcher matcher = SCHEMA_PATTERN_3.matcher(selectSql);
            if (matcher.find()) {
                StringBuilder builder = new StringBuilder(selectSql.substring(0, matcher.end()));
                builder.append(schema + ".");
                builder.append(selectSql.substring(matcher.end()));
                selectSql = builder.toString();
            }
        }

        return selectSql;
    }

    //insert into hive.tablen (col1,col2,xxx) or insert into hive.tablen
    public static String getHiveInsertSQL(String sql, boolean hasSchema, String schema) throws UDFArgumentException {
        if (sql == null || sql.length() == 0) {
            throw new UDFArgumentException("not a valid query sql!");
        }
        int selectIndex = sql.toLowerCase().indexOf("select");  //注意大小写
        if (selectIndex == -1) {
            throw new UDFArgumentException("not a valid query sql!");
        }
        String selectSql = sql.substring(0, selectIndex);

        if (schema != null && schema.length() != 0) {
            Matcher matcher = INSERT_INTO_PATTERN_3.matcher(selectSql);
            if (matcher.find() && hasSchema) {
                StringBuilder builder = new StringBuilder(selectSql.substring(0, matcher.end()));
                builder.append(schema + ".");
                builder.append(selectSql.substring(matcher.end()));
                selectSql = builder.toString();
            }
        }

        return selectSql;
    }


    //insert into hive.tablen (col1,col2,xxx) or insert into hive.tablen
    public static String getHiveInsertSQL(String sql, String schema) throws UDFArgumentException {
        return getHiveInsertSQL(sql, false, schema);
    }

}
