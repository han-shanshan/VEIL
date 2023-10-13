package experiments;
import DataProcessing.DBOwner;
import static Utils.Constant.*;

public class Test_BaseClass {
    public DBOwner owner;

    public Test_BaseClass(String dbName) {
        owner = new DBOwner(dbName);
    }

    public String getOverlappingTableName_multimap(int fanout) {
        return RANDOM_BUCKETING_OVERLAPPING_MULTIMAP_TABLE + "_" + owner.dbName + "_" + fanout;
    }

    String getDisjointTableName_multimap(int fanout) {
        return RANDOM_BUCKETING_DISJOINT_MULTIMAP_TABLE + "_" + owner.dbName + "_" + fanout;
    }




}
