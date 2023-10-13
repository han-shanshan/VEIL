package experiments;
import Modules.Bucket;
import java.util.*;

import static Utils.Constant.MULTIMAP_FOR_OVERLAPPING;
import static Utils.Constant.LOCAL_STASH_TABLE_NAME;

public class Test_MultiMap_Overlapping extends Test_BaseClass {

    public Test_MultiMap_Overlapping(String dbName)  {
        super(dbName);
    }

    /**
     * create overlapping buckets and outsource
     */
    public void set_up(String dataTableName, String keyField, String valField, double SA, double QA, int fanout, int degree) {
        // read the KV dataset; store the dataset into a map
        Map<String, List<String>> dataset = owner.dq.read_a_dataset(dataTableName, keyField, valField);
        // random bucketing: allocate key-value pairs in a greedy way
        ArrayList<Bucket> buckets = owner.allocate_keys_to_buckets_multimap(dataset, SA, QA, fanout);
        // create a cluster using the buckets and add fake records
        buckets = owner.create_a_d_regular_graph_and_padding(buckets, degree);
        // outsource the buckets
        owner.outsource_for_random_bucket_multimap(buckets, getOverlappingTableName_multimap(fanout));

        for (String key : dataset.keySet()) {
            dataset.get(key).clear();
        }
        dataset.clear();
        buckets.clear();
    }

    /**
     * query
     */
    private void query(int fanout, int degree) {
        int keyNum = 20;
        String queryTableName = getOverlappingTableName_multimap(fanout);
        String local_map_table_name = MULTIMAP_FOR_OVERLAPPING + "_" + degree;
        int total_bucket_num = owner.dq.getTotalBucketNum(LOCAL_STASH_TABLE_NAME);
        Map<String, List<String>> local_stash = owner.dq.getLocalStash(LOCAL_STASH_TABLE_NAME);
        String[] queryKeys = new String[keyNum];
        Random r = new Random();
        ArrayList<String> allKeys = owner.dq.query_for_distinct_keys_from_lineitem_table();
        for(int i = 0; i < keyNum; i++) {
            queryKeys[i] = allKeys.get(r.nextInt(allKeys.size()));
        }
        for(String queryKey: queryKeys) {
            ArrayList<String> result = owner.query_a_key_multimap(queryKey, queryTableName, local_map_table_name, fanout, total_bucket_num);
            if(local_stash.containsKey(queryKey)) {
                result.addAll(local_stash.get(queryKey));
            }
        }
    }


    public static void main(String[] args) {
        String dbName = "tpch_6m";
        int fanout = 6; // {3, 6, 9, 12, 15};
        double SA = 1.2, QA = 1.0;
        int degree = 2;
        new Test_MultiMap_Overlapping(dbName).set_up("LINEITEM", "L_PARTKEY", "L_SUPPKEY", SA, QA, fanout, degree);
        new Test_MultiMap_Overlapping(dbName).query(fanout, degree);
    }
}
