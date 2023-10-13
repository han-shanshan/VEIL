package experiments;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static Utils.Constant.LOCAL_MAP_TABLE_DPRFMM;

public class Test_DprfMM extends Test_BaseClass {

    public Test_DprfMM(String dbName)  {
        super(dbName);
    }

    /**
     * DPRFMM approach
     */
    public void dprfMM(String tableName, String keyField, String valField, String outsourceTableName, double alpha) {
        Map<String, List<String>> dataset = owner.dq.read_a_dataset(tableName, keyField, valField);
        owner.dprfMM(dataset, outsourceTableName, alpha);
        for(String key: dataset.keySet()) {
            dataset.get(key).clear();
        }
        dataset.clear();
    }

    private void testQueries_dprfMM(String queryTableName, double alpha) {
        int keyNum = 20;
        String[] queryKeys = new String[keyNum];
        // get max key size
        int Lmax = owner.dq.getLmax(LOCAL_MAP_TABLE_DPRFMM);
        // get table size
        int tableSize = owner.dq.getTableSizeForXorMM(LOCAL_MAP_TABLE_DPRFMM, alpha);
        Random r = new Random();
        // get keys from kv dataset
        ArrayList<String> allKeys = owner.dq.query_for_distinct_keys_from_lineitem_table();
        for(int i = 0; i < keyNum; i++) {
            // randomly select 20 keys to query
            queryKeys[i] = allKeys.get(r.nextInt(allKeys.size()));
        }
        for(String queryKey: queryKeys) {
            owner.query_a_key_from_dprfMM(queryKey, queryTableName, Lmax, tableSize);
        }
    }

    public static void main(String[] args) {
        String dbName = "tpch_6m";
        double alpha = 0;

        String dprfMM_table_name = "dprfMM" + dbName + 0+"_" + ((int)(alpha * 10));
        new Test_DprfMM(dbName).dprfMM("LINEITEM", "L_PARTKEY", "ALL", dprfMM_table_name, alpha);
        new Test_DprfMM(dbName).testQueries_dprfMM(dprfMM_table_name, alpha);
    }
}
