package DBOperation.PostgreSQLOperation;
import java.sql.Connection;
import java.sql.DriverManager;

public class DBConnector {
    Connection conn = null;
    public DBConnector(String dbName){
        String user = "test";
        String password = "test";
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + dbName.toLowerCase(), user, password); // ,"postgres", "123");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
    }
}