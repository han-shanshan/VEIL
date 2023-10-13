package DBOperation.PostgreSQLOperation;
import Modules.KVPair;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

public class DBInsert {
    private DBConnector connector;
    public DBInsert(DBConnector connector){
        this.connector = connector;
    }

    public void insert_a_doc(String tableName, String keyField, String valField, KVPair kv, boolean isIntKey){
        String SQL = "INSERT INTO " + tableName + "(" + keyField + ", " + valField + ") "
                + "VALUES(?,?)";
        try (
                PreparedStatement pstmt = this.connector.conn.prepareStatement(SQL)) {
            if (isIntKey) {
                pstmt.setInt(1, Integer.parseInt(kv.key));
            } else {
                pstmt.setString(1, kv.key);
            }
            pstmt.setString(2, kv.value);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public int insert_docs(String tableName, String keyField, String valField, ArrayList<KVPair> kvArray, boolean isIntKey){
        String SQL = "INSERT INTO " + tableName + "(" + keyField + ", " + valField + ") "
                + "VALUES(?,?)";
        int counter = 0;
        try (
                PreparedStatement pstmt = this.connector.conn.prepareStatement(SQL)) {
            if (isIntKey) {
                for(KVPair kv: kvArray) {
                    pstmt.setInt(1, Integer.parseInt(kv.key));
                    pstmt.setString(2, kv.value);
                    pstmt.addBatch();
                    counter++;
                    if (counter % 100 == 0 || counter == kvArray.size()) {
                        pstmt.executeBatch();
                    }
                }
            } else {
                for(KVPair kv: kvArray) {
                    pstmt.setString(1, kv.key);
                    pstmt.setString(2, kv.value);
                    pstmt.addBatch();
                    counter++;
                    if (counter % 100 == 0 || counter == kvArray.size()) {
                        pstmt.executeBatch();
                    }
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return counter;
    }

    public int insert_bytes_EEM_xormm(String tableName, String keyField, String valField, byte[][] EEM, boolean isIntKey){
        String SQL = "INSERT INTO " + tableName + "(" + keyField + ", " + valField + ") "
                + "VALUES(?,?)";
        int counter = 0;
        try (
                PreparedStatement pstmt = this.connector.conn.prepareStatement(SQL)) {
            if (isIntKey) {
                for(int i = 0; i < EEM.length; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setBytes(2, EEM[i]);
                    pstmt.addBatch();
                    counter++;
                    if (counter % 100 == 0 || counter == EEM.length) {
                        pstmt.executeBatch();
                    }
                }
            } else {
                for(int i = 0; i < EEM.length; i++) {
                    pstmt.setString(1, i + "");
                    pstmt.setBytes(2, EEM[i]);
                    pstmt.addBatch();
                    counter++;
                    if (counter % 100 == 0 || counter == EEM[i].length) {
                        pstmt.executeBatch();
                    }
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return counter;
    }
}
