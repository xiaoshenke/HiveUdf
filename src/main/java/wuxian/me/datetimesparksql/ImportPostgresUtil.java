package wuxian.me.datetimesparksql;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import java.sql.ResultSet;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.getQualifiedTableName;
import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.readProps;

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

    //Todo
    public static boolean insertHiveTableBy(Driver driver, String insertSql, ResultSet resultSet) {
        if (driver == null || insertSql == null || insertSql.length() == 0 || resultSet == null) {
            return false;
        }
        CommandProcessorResponse response = null;
        String sql = "show databases"; //Todo:组合一下sql 然后循环执行
        try {
            response = driver.run(sql);
        } catch (Exception e) {
            return false;
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


    //Todo
    public static ResultSet getSelectPGResult(String selectSQL) throws UDFArgumentException {
        return null;
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
