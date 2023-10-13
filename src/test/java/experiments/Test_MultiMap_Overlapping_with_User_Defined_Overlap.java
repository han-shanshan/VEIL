package experiments;
import Modules.Bucket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import static Utils.Constant.LOCAL_STASH_TABLE_NAME;
import static Utils.Constant.MULTIMAP_FOR_OVERLAPPING;

public class Test_MultiMap_Overlapping_with_User_Defined_Overlap extends Test_BaseClass {

    public Test_MultiMap_Overlapping_with_User_Defined_Overlap(String dbName)  {
        super(dbName);
    }

    public void set_up(String dataTableName, String keyField, String valField, double SA, double QA, int fanout, int degree, int desired_overlapping_size) {
        Map<String, List<String>> dataset = owner.dq.read_a_dataset(dataTableName, keyField, valField);
        // random bucketing
        ArrayList<Bucket> bins = owner.allocate_keys_to_buckets_multimap(dataset, SA, QA, fanout);
        // create overlapping buckets, add fake values, and encrypt
        bins = owner.create_a_d_regular_graph_and_pad_with_given_desired_overlapping_size(bins, degree, desired_overlapping_size);
        // outsource
        owner.outsource_for_random_bucket_multimap(bins, getOverlappingTableName_multimap(fanout));
        for (String key : dataset.keySet()) {
            dataset.get(key).clear();
        }
        dataset.clear();
        bins.clear();
    }

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
        String dbName = "tpch_6mz0_4";
        int fanout = 6; // {3, 6, 9, 12, 15};
        double SA = 1.2;
        double QA = 1;
        int desired_overlapping_size = 2;
        int degree = 2;
        new Test_MultiMap_Overlapping_with_User_Defined_Overlap(dbName).set_up("LINEITEM", "L_PARTKEY", "L_SUPPKEY", SA, QA, fanout, degree, desired_overlapping_size);
        new Test_MultiMap_Overlapping_with_User_Defined_Overlap(dbName).query(fanout, degree);
    }
}
