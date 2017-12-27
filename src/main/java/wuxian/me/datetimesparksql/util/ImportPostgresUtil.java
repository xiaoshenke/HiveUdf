package wuxian.me.datetimesparksql.util;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import java.sql.*;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.getQualifiedTableName;

public class ImportPostgresUtil {

    private ImportPostgresUtil() {
    }

    //current only support
    //insert into hive.table1 select * from postgres.table2;
    //Todo:
    //1 select子句的column数量和insert into子句的column数量是否相同
    //2 select子句的所有column类型和insert into子句的所有column类型是否相同 --> 目前从编译阶段检查比较困难,因此先弱化成运行时检查
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


    //Todo: 这里可能会出bug pg的type和hive的type是否不兼容？
    public static String combineSql(String insertSql, ResultSetMetaData metaData, ResultSet resultSet) throws SQLException {
        StringBuilder builder = new StringBuilder(insertSql + " values(");
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            if (metaData.getColumnClassName(i).equalsIgnoreCase("java.String")) {
                builder.append("'" + resultSet.getString(i) + "'");
            } else {
                builder.append(resultSet.getString(i));  //string is safe
            }
            if (i + 1 <= metaData.getColumnCount()) {
                builder.append(",");
            }
        }
        builder.append(");");
        return builder.toString();
    }

    public static boolean insertHiveTableBy(Driver driver, String insertSql, ResultSet resultSet) throws SQLException {
        if (driver == null || insertSql == null || insertSql.length() == 0 || resultSet == null) {
            return false;
        }
        ResultSetMetaData metaData = resultSet.getMetaData();
        CommandProcessorResponse response = null;
        while (resultSet.next()) {
            String sql = combineSql(insertSql, metaData, resultSet);
            try {
                response = driver.run(sql);
            } catch (Exception e) {
                return false;
            }
        }
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

    public static ResultSet getSelectPGResult(String selectSQL, Connection connection) throws UDFArgumentException, SQLException {
        PreparedStatement pstmt = connection.prepareStatement(selectSQL);
        ResultSet resultSet = pstmt.executeQuery();
        if (pstmt != null) {
            pstmt.close();
        }
        return resultSet;
    }

    public static String getPGSelectSQL(String sql) throws UDFArgumentException {
        if (sql == null || sql.length() == 0) {
            throw new UDFArgumentException("not a valid query sql!");
        }
        int selectIndex = sql.toLowerCase().indexOf("select");  //注意大小写
        if (selectIndex == -1) {
            throw new UDFArgumentException("not a valid query sql!");
        }
        String selectSql = sql.substring(selectIndex, sql.length());
        return selectSql;
    }

    //insert into hive.tablen (col1,col2,xxx) or insert into hive.tablen
    public static String getHiveInsertSQL(String sql) throws UDFArgumentException {
        if (sql == null || sql.length() == 0) {
            throw new UDFArgumentException("not a valid query sql!");
        }
        int selectIndex = sql.toLowerCase().indexOf("select");  //注意大小写
        if (selectIndex == -1) {
            throw new UDFArgumentException("not a valid query sql!");
        }
        String selectSql = sql.substring(0, selectIndex);
        return selectSql;
    }

}
