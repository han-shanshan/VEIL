package DataProcessing;
import DBOperation.PostgreSQLOperation.DBOperator;
import Modules.Bucket;
import Modules.KVPair;
import Utils.Constant;
import java.util.ArrayList;

public class DataOutsourcing {
    private DBOperator ope;

    public DataOutsourcing(DBOperator ope){
        this.ope = ope;
    }

    public int store_multi_map(String localMapTable, ArrayList<KVPair> KVPairs) {
        this.ope.create_a_table_for_local_storage(localMapTable); // create a table
        int counter = ope.DBInsert.insert_docs(localMapTable, Constant.KEY_COLUMN, Constant.VALUE_COLUMN, KVPairs, false);
        ope.createIndex(localMapTable, Constant.KEY_COLUMN);
        return counter;
    }

    public int storeLocalStash(String tableName, ArrayList<KVPair> KVPairs) {
        this.ope.create_a_table_for_local_storage(tableName);
        int counter = ope.DBInsert.insert_docs(tableName, Constant.KEY_COLUMN, Constant.VALUE_COLUMN, KVPairs, false);
        ope.createIndex(tableName, Constant.KEY_COLUMN);
        return counter;
    }

    public int store_cached_value_to_local_stash(String tableName, String key, String value) {
        ope.DBInsert.insert_a_doc(tableName, Constant.KEY_COLUMN, Constant.VALUE_COLUMN, new KVPair(key, value), false);
        return 0;
    }

    public int outsource_random_bucketing_multimap(ArrayList<Bucket> bins, String tableName) {
        ArrayList<KVPair> KVList = new ArrayList<>();
        for(Bucket b: bins) {
            for(int i = 0; i < b.enc_kv_pairs_for_multimap.size(); i++){
                KVList.add(new KVPair(b.enc_kv_pairs_for_multimap.get(i).key, b.enc_kv_pairs_for_multimap.get(i).value));
            }
        }
        ope.createDataTable_multimap(tableName);
        int num = ope.DBInsert.insert_docs(tableName, Constant.DATA_TABLE_KEY_FIELD, Constant.DATA_TABLE_VAL_FIELD, KVList, false);
        ope.createIndex(tableName, Constant.DATA_TABLE_KEY_FIELD);
        return num;
    }

    public int outsource_Moti(ArrayList<KVPair> kvList, String tableName) {
        return ope.DBInsert.insert_docs(tableName, Constant.DATA_TABLE_KEY_FIELD, Constant.DATA_TABLE_VAL_FIELD, kvList, false);
    }

    public int outsource_xormm(byte[][] EEM, String tableName) {
        return ope.DBInsert.insert_bytes_EEM_xormm(tableName, Constant.DATA_TABLE_KEY_FIELD, Constant.DATA_TABLE_VAL_FIELD, EEM, true);
    }
}
