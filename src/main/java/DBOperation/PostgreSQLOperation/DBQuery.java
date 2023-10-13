package DBOperation.PostgreSQLOperation;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static Utils.Constant.*;

public class DBQuery {
    private DBConnector connector;
    public DBQuery(DBConnector connector){
        this.connector = connector;
    }

    public ArrayList<byte[]> query_using_integer_query_tokens_xormm(String tableName, String keyField, ArrayList<Integer> queryTokens, String valField) {
        String SQL = "";
        ArrayList<byte[]> results = new ArrayList<>();
        SQL = "select " + keyField + ", " + valField + " from " + tableName + " where " + keyField + " in (" ;
        for(int i = 0; i < queryTokens.size(); i++) {
            SQL = SQL + queryTokens.get(i);
            if(i<queryTokens.size() - 1) {SQL += ", "; }
        }
        SQL += ")";
        try {
            Statement stmt = this.connector.conn.createStatement();
            ResultSet rs = stmt.executeQuery(SQL);
            while (rs.next() ) {
                results.add(rs.getBytes(valField));
            }
            rs.close();
            stmt.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        return results;
    }

    public ArrayList<String> query_using_string_query_tokens(String tableName, String keyField, ArrayList<String> queryTokens, String valField) {
        String SQL = "";
        ArrayList<String> results = new ArrayList<>();
        SQL = "select " + keyField + ", " + valField + " from " + tableName + " where " + keyField + " in (" ;
        for(int i = 0; i < queryTokens.size(); i++) {
            SQL = SQL +  "'" + queryTokens.get(i) + "'";
            if(i<queryTokens.size() - 1) {SQL += ", "; }
        }
        SQL += ")";
        String value;
        try {
            Statement stmt = this.connector.conn.createStatement();
            ResultSet rs = stmt.executeQuery(SQL);
            while (rs.next() ) {
                value = rs.getString(valField);
                results.add(value);
            }
            rs.close();
            stmt.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        return results;
    }

    public Map<String, List<String>> query_for_local_stash_overlapping_approach(String tableName) {
        String  value= "", key = "";
        String SQL = "select " + KEY_COLUMN + ", " +  VALUE_COLUMN + " from " + tableName + " where " + KEY_COLUMN +
                " not in ('" + LOCAL_MAP_META_FOR_TOTAL_BIN_NUM + "')";
        Map<String, List<String>> stash = new HashMap<>();
        try {
            Statement stmt = this.connector.conn.createStatement();
            ResultSet rs = stmt.executeQuery(SQL);
            while (rs.next() ) {
                key = rs.getString(KEY_COLUMN);
                value = rs.getString(VALUE_COLUMN);
                if(!stash.containsKey(key)) {stash.put(key, new ArrayList<>());}
                stash.get(key).add(value);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        return stash;
    }

    public int query_from_local_map(String tableName, String query_attr) {
        String result = "";
        String SQL = "select " + VALUE_COLUMN + " from " + tableName + " where " + KEY_COLUMN + " = '" + query_attr + "'";;
        try {
            Statement stmt = this.connector.conn.createStatement();
            ResultSet rs = stmt.executeQuery(SQL);
            if (rs.next() ) {
                result = rs.getString(VALUE_COLUMN);
            }
            rs.close();
            stmt.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        return Integer.parseInt(result);
    }

    public ArrayList<String > query_for_distinct_keys_from_lineitem_table() {
        ArrayList<String> result = new ArrayList<>();
        String SQL = "select distinct l_partkey from lineitem";
        try {
            Statement stmt = this.connector.conn.createStatement();
            ResultSet rs = stmt.executeQuery(SQL);
            while (rs.next() ) {
                result.add(rs.getString("l_partkey"));
            }
            rs.close();
            stmt.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        return result;
    }

    /**
     * read the KV dataset
     */
    public Map<String, List<String>> get_kv_dataset(String tableName, String keyField, String valField) {
        Map<String, List<String>> dataset = new HashMap<>();
        String key, value;

        String SQL = "";
        if(valField.equals("ALL")) {
            SQL = "select * from " + tableName;
        } else {
            SQL = "select " + keyField + ", " + valField + " from " + tableName;
        }
        try {
            Statement stmt = this.connector.conn.createStatement();
            ResultSet rs = stmt.executeQuery(SQL);
            int max_len = 0;
            if(valField.equals("ALL")) {
                while (rs.next()) {
                    key = Integer.toString(rs.getInt(keyField));
                    String valueStr = "";
                    valueStr = Integer.toString(rs.getInt("l_orderkey")).trim();
                    valueStr = valueStr + rs.getInt("l_suppkey");
                    valueStr = valueStr + rs.getInt("l_linenumber");
                    valueStr = valueStr + rs.getDouble("l_quantity");
                    valueStr = valueStr + rs.getDouble("l_extendedprice");
                    valueStr = valueStr + rs.getDouble("l_discount");
                    valueStr = valueStr + rs.getDouble("l_tax");
                    valueStr = valueStr + rs.getString("l_returnflag").trim();
                    valueStr = valueStr + rs.getString("l_linestatus").trim();
                    valueStr = valueStr + rs.getDate("l_shipdate");
                    valueStr = valueStr + rs.getDate("l_commitdate");
                    valueStr = valueStr + rs.getDate("l_receiptdate");
                    valueStr = valueStr + rs.getString("l_shipinstruct").trim();
                    valueStr = valueStr + rs.getString("l_shipmode").trim();
                    valueStr = valueStr + rs.getString("l_comment").trim();

                    if (!dataset.containsKey(key)) {
                        dataset.put(key, new ArrayList<>());
                    }
                    if(max_len < valueStr.length()) {
                        max_len = valueStr.length();
                    }
                    dataset.get(key).add(valueStr);
                }
            } else {
                while (rs.next()) {
                    key = rs.getString(keyField);
                    value = rs.getString(valField);
                    if (!dataset.containsKey(key)) {
                        dataset.put(key, new ArrayList<>());
                    }
                    dataset.get(key).add(value);
                }
            }
            rs.close();
            stmt.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        return dataset;
    }
}
