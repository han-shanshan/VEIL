package DBOperation.PostgreSQLOperation;
import Utils.Constant;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBOperator {
    private DBConnector conn;
    public DBOperation.PostgreSQLOperation.DBQuery DBQuery;
    public DBOperation.PostgreSQLOperation.DBInsert DBInsert;
    public FileWriter logWriter = null;

    public DBOperator(String dbName){
        this.conn = new DBConnector(dbName);
        this.DBQuery = new DBQuery(this.conn);
        this.DBInsert = new DBInsert(this.conn);
    }

    public void close() {
        if(logWriter != null) {
            try { logWriter.close(); } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try { this.conn.conn.close(); } catch (SQLException e) {}
    }

    public void createIndex(String tableName, String field) {
        try {
        Statement stmt = null;
        stmt = this.conn.conn.createStatement();
        String sql = "CREATE INDEX idx_" + tableName + "_" + field + " ON " + tableName + "(" + field + ")";
        stmt.executeUpdate(sql);
        stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createOverlappingBucketTable(String tableName) {
        createDataTable(tableName, false);
    }

    public void createDataTable_multimap(String tableName) {
        createDataTable(tableName, false);
    }

    public void create_a_table_for_local_storage(String mapTableName){
        deleteTableIfExists(mapTableName);

        try {
            Statement stmt = this.conn.conn.createStatement();
            String sql = "create Table " + mapTableName + " (" + Constant.KEY_COLUMN + " varchar(50) not null, " + Constant.VALUE_COLUMN
                    + " varchar(500000) not null )";
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void deleteTableIfExists(String tableName) {
        try {
            DatabaseMetaData dbm = this.conn.conn.getMetaData();
            ResultSet tables = dbm.getTables(null, null, tableName.toLowerCase(), null);
            if (tables.next()) {
                Statement stmt = this.conn.conn.createStatement();
                String sql = "DROP TABLE " + tableName;
                stmt.executeUpdate(sql);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createDataTable(String tableName, boolean isKeyInt){
        deleteTableIfExists(tableName);
        try {
        Statement stmt = this.conn.conn.createStatement();

        String sql = "create Table " + tableName + " (key ";
        if(isKeyInt) {sql += " integer "; }
        else {sql += " varchar(50) ";}
        sql += " not null, value varchar(500000) )";

        stmt.executeUpdate(sql);
        stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createDataTable_xormm(String tableName){
        deleteTableIfExists(tableName);
        try {
            Statement stmt = this.conn.conn.createStatement();
            String sql = "create Table " + tableName + " (key integer not null, value BYTEA )";
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

