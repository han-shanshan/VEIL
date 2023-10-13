package experiments;

import Modules.Bucket;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static Utils.Constant.*;

public class Test_MultiMap_Disjoint extends Test_BaseClass {

    public Test_MultiMap_Disjoint(String dbName)  {
        super(dbName);
    }

    public void set_up(String tableName, String keyField, String valField, double SA, double QA, int fanout) {
        Map<String, List<String>> dataset = owner.dq.read_a_dataset(tableName, keyField, valField);
        ArrayList<Bucket> bins = owner.allocate_keys_to_buckets_multimap(dataset, SA, QA, fanout);
        bins = owner.add_fake_values_disjoint_buckets(bins);
        owner.outsource_for_random_bucket_multimap(bins, getDisjointTableName_multimap(fanout));
        for (String key : dataset.keySet()) {
            dataset.get(key).clear();
        }
        dataset.clear();
        bins.clear();
    }

    private void query(int fanout) {
        int keyNum = 20;
        String queryTableName = getDisjointTableName_multimap(fanout);

        int total_bucket_num = owner.dq.getTotalBucketNum(LOCAL_STASH_TABLE_NAME);
        Map<String, List<String>> local_stash = owner.dq.getLocalStash(LOCAL_STASH_TABLE_NAME);
        String[] queryKeys = new String[keyNum];
        Random r = new Random();
        ArrayList<String> allKeys = owner.dq.query_for_distinct_keys_from_lineitem_table();
        for(int i = 0; i < keyNum; i++) {
            queryKeys[i] = allKeys.get(r.nextInt(allKeys.size()));
        }
        for(String queryKey: queryKeys) {
            ArrayList<String> result = owner.query_a_key_multimap(queryKey, queryTableName, MULTIMAP_FOR_DISJOINT, fanout, total_bucket_num);
            if(local_stash.containsKey(queryKey)) {
                result.addAll(local_stash.get(queryKey));
            }
        }
    }

    public static void main(String[] args) {
        String dbName = "tpch_6m";
        int fanout = 6;
        double SA = 2;
        double QA = 1.0;

        new Test_MultiMap_Disjoint(dbName).set_up("LINEITEM", "L_PARTKEY", "ALL", SA, QA, fanout);
        new Test_MultiMap_Disjoint(dbName).query(fanout);
    }
}
