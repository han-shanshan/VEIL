package DataProcessing;
import DBOperation.PostgreSQLOperation.DBOperator;
import Utils.AES;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static Utils.Constant.*;

public class DataQuery {
    private DBOperator ope;

    public DataQuery(DBOperator ope) {
        this.ope = ope;
    }

    public Map<String, List<String>> read_a_dataset(String tableName, String keyField, String valField) {
        return ope.DBQuery.get_kv_dataset(tableName, keyField, valField);
    }

    public ArrayList<String> query_for_distinct_keys_from_lineitem_table() {
        return this.ope.DBQuery.query_for_distinct_keys_from_lineitem_table();
    }

    public int getTotalBucketNum(String tableName) {
        return this.ope.DBQuery.query_from_local_map(tableName, LOCAL_MAP_META_FOR_TOTAL_BIN_NUM);
    }

    public int getLmax(String tableName) {
        return this.ope.DBQuery.query_from_local_map(tableName, LOCAL_MAP_META_FOR_LMAX);
    }

    //
    public int getTableSizeForXorMM(String tableName, double alpha) {
        if (alpha == 0.0) {
            return this.ope.DBQuery.query_from_local_map(tableName, LOCAL_MAP_META_FOR_TABLESIZE_0);
        } else if (alpha == 0.3) {
            return this.ope.DBQuery.query_from_local_map(tableName, LOCAL_MAP_META_FOR_TABLESIZE_0_3);
        }
        else{
            System.out.println("no table size! " + 0 );
            return 0;
        }
    }

    public Map<String, List<String>> getLocalStash(String tableName) {
        return this.ope.DBQuery.query_for_local_stash_overlapping_approach(tableName);
    }

    public ArrayList<byte[]> get_values_using_integer_tokens_xormm(String tableName, String keyField, String valField, ArrayList<Integer> query_tokens) {
        return ope.DBQuery.query_using_integer_query_tokens_xormm(tableName, keyField, query_tokens, valField);
    }

    public ArrayList<String> get_values_using_string_tokens(String tableName, String keyField, String valField, ArrayList<String> query_tokens) {
        return ope.DBQuery.query_using_string_query_tokens(tableName, keyField, query_tokens, valField);
    }

    public ArrayList<String> get_results_from_encrypted_strings(String queryKey, ArrayList<String> encBin) {
        ArrayList<String> queryResults = new ArrayList<>();
        String key;
        for(String s: encBin) {
            key = AES.decrypt(s).split("\\|\\|",0)[0];
            if(key.equals(queryKey)) {
                queryResults.add(AES.decrypt(s).split("\\|\\|",0)[1]);
            }
        }
        return queryResults;
    }
}
