package DataProcessing;
import DBOperation.PostgreSQLOperation.DBOperator;
import Modules.Bucket;
import Modules.KVPair;
import Utils.AES;
import Utils.Constant;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import static Utils.Constant.*;

public class DBOwner {
    public String dbName;
    public Map<String, String> meta;
    public int max_key_size, delta;
    public DBOperator ope;
    public DataQuery dq;
    public NumberFormat formatter = new DecimalFormat("#0.00000");
    public SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
    public int bucket_size;

    public DBOwner(String dbName) {
        this.dbName = dbName;
        this.ope = new DBOperator(dbName);
        dq = new DataQuery(this.ope);
        this.meta = new HashMap<>();
    }

    public int getDatasize(Map<String, List<String>> dataset) {
        int datasize = 0;
        for(String key: dataset.keySet()) {
            datasize += dataset.get(key).size();
        }
        return datasize;
    }

    /**
     * outsource the encrypted buckets for multimap -- overlapping and disjoint
     */
    public void outsource_for_random_bucket_multimap(ArrayList<Bucket> binList, String tableName) {
        DataOutsourcing dataOutsourcing = new DataOutsourcing(this.ope);
        dataOutsourcing.outsource_random_bucketing_multimap(binList, tableName);
    }

    public int outsource_dataset_xormm(byte[][] EEM, String tableName) {
        DataOutsourcing dataOutsourcing = new DataOutsourcing(this.ope);
        this.ope.createDataTable_xormm(tableName); // create a table
        int DBsize = dataOutsourcing.outsource_xormm(EEM, tableName);
        ope.createIndex(tableName, Constant.DATA_TABLE_KEY_FIELD);
        return DBsize;
    }

    public void outsource_dprfMM(ArrayList<KVPair> kvList, String tableName) {
        DataOutsourcing dataOutsourcing = new DataOutsourcing(this.ope);
        this.ope.createOverlappingBucketTable(tableName);
        dataOutsourcing.outsource_Moti(kvList, tableName);
        ope.createIndex(tableName, Constant.DATA_TABLE_KEY_FIELD);
    }

    public ArrayList<Bucket> create_a_d_regular_graph_and_padding(ArrayList<Bucket> buckets, int degree) {
        OverlappingBinning overlappingBinning = new OverlappingBinning(this.bucket_size);
        DataOutsourcing dataOutsourcing = new DataOutsourcing(ope);
        // create a d-regular graph
        ArrayList<KVPair> map = overlappingBinning.create_d_regular_graph_multimap(buckets, degree, 0);
        dataOutsourcing.store_multi_map(MULTIMAP_FOR_OVERLAPPING + "_" + degree, map);
        dataOutsourcing.store_cached_value_to_local_stash(LOCAL_STASH_TABLE_NAME, LOCAL_MAP_META_FOR_TOTAL_BIN_NUM, buckets.size() + "");
        return buckets;
    }

    /**
     * create a d-regular graph and pad with a user-desired overlapping size
     */
    public ArrayList<Bucket> create_a_d_regular_graph_and_pad_with_given_desired_overlapping_size(ArrayList<Bucket> buckets, int degree, int desired_overlapping_size) {
        OverlappingBinning overlappingBinning = new OverlappingBinning(this.bucket_size);
        DataOutsourcing dataOutsourcing = new DataOutsourcing(ope);
        ArrayList<KVPair> map = overlappingBinning.create_a_d_regular_graph_with_given_desired_overlapping_size(buckets, degree, desired_overlapping_size);
        dataOutsourcing.store_multi_map(MULTIMAP_FOR_OVERLAPPING + "_" + degree, map);
        dataOutsourcing.store_cached_value_to_local_stash(LOCAL_STASH_TABLE_NAME, LOCAL_MAP_META_FOR_TOTAL_BIN_NUM, buckets.size() + "");
        return buckets;
    }

    /**
     * padding
     */
    public ArrayList<Bucket> add_fake_values_disjoint_buckets(ArrayList<Bucket> buckets) {
        String fake_key = "fake_key", fake_value = "fake_value";
        Random r = new Random();
        int random_num, random_num2;

        ArrayList<KVPair> map = new ArrayList<>();
        for (Bucket b : buckets) {
            StringBuilder temp_meta = new StringBuilder();
            for(int i = 0; i < b.binLoad; i++) { // generate multi-map for the bucket b
                temp_meta.append(b.enc_kv_pairs_for_multimap.get(i).key).append("_");
            }
            for (int i = 0; i < this.bucket_size - b.binLoad; i++) { // add fake records and add the ids for the fake records to the multimap
                random_num = r.nextInt();
                random_num2 = r.nextInt();
                b.enc_kv_pairs_for_multimap.add(new KVPair(random_num, AES.concatAndEncrypt(fake_key + random_num2, fake_value + random_num2)));
                temp_meta.append(random_num).append("_");
            }
            map.add(new KVPair(b.binID, temp_meta.toString()));
        }
        DataOutsourcing dataOutsourcing = new DataOutsourcing(ope);
        dataOutsourcing.store_multi_map(MULTIMAP_FOR_DISJOINT, map);
        dataOutsourcing.store_cached_value_to_local_stash(LOCAL_STASH_TABLE_NAME, LOCAL_MAP_META_FOR_TOTAL_BIN_NUM, buckets.size() + "");

        return buckets;
    }

    /**
     * find the bucket id for a record in a greedy way, i.e., choose the smallest one
     */
    public int get_the_smallest_bucket_id_for_a_record(String key, int fanout, ArrayList<Bucket> buckets, Map<String, ArrayList<Integer>> hashed_bucket_ids){
        int bucket_id;
        int temp_load = this.bucket_size;
        int temp_bid = -1;
        for (int j = 0; j < fanout; j++) {
            bucket_id = hashed_bucket_ids.get(key).get(j);
            if(temp_bid == -1) {
                temp_bid = bucket_id;
            }
            if (buckets.get(bucket_id).binLoad < temp_load) {
                temp_bid = bucket_id;
                temp_load = buckets.get(bucket_id).binLoad;
            }
        }
        return temp_bid;
    }

    /**
     * allocate keys to buckets for each value; using multimap
     */
    public ArrayList<Bucket> allocate_keys_to_buckets_multimap(Map<String, List<String>> dataset, double SA, double QA, int fanout) {
        this.max_key_size = this.get_max_keysize(dataset); // compute Lmax
        int datasize = this.getDatasize(dataset); // get datasize
        ArrayList<KVPair> stash = new ArrayList<>();
        Map<String, ArrayList<Integer>> hashed_bucket_ids = new HashMap<>();
        int total_bucket_num;
        List<KVPair> D = new ArrayList<>();
        for (String key: dataset.keySet()) {
            int keysize = dataset.get(key).size(); //get the key size
            for(int j = 0; j < keysize; j++) {
                D.add(new KVPair(key, dataset.get(key).get(j)));
            }
        }
        Collections.shuffle(D); // mix the KV pairs
        ArrayList<Bucket> buckets = new ArrayList<>();
        this.bucket_size = (int) Math.ceil(((this.max_key_size * QA) / fanout));
        total_bucket_num = (int) Math.ceil( SA * (double) datasize / this.bucket_size );
        buckets = create_empty_buckets(total_bucket_num);

        for(String key: dataset.keySet()) {
            hashed_bucket_ids.put(key, new ArrayList<Integer>());
            for(int idx = 0; idx < fanout; idx++) {
                hashed_bucket_ids.get(key).add(computeHashPosition(key + "_" + idx, total_bucket_num));
            }
        }
        // compute total eviction number
        int total_eviction_num = 5 * (int) Math.ceil(Math.log(datasize) / Math.log(2.0));
        Random r = new Random();
        KVPair temp_record = new KVPair();
        int eviction_counter;
        boolean is_finished;
        int temp_bid = -1;
        for (KVPair record: D) { //for each record in the dataset
            is_finished = false;
            temp_bid = get_the_smallest_bucket_id_for_a_record(record.key, fanout, buckets, hashed_bucket_ids);
            buckets.get(temp_bid).plaintext_kv_pairs_for_multimap.add(new KVPair(record.key, record.value));
            if (buckets.get(temp_bid).binLoad < this.bucket_size) {
                buckets.get(temp_bid).binLoad++;
            } else{
                int rand_idx = -1;
                for(eviction_counter = 0; eviction_counter < total_eviction_num;) {
                    // ranomly remove a record from the current bucket
                    rand_idx = r.nextInt(buckets.get(temp_bid).plaintext_kv_pairs_for_multimap.size() - 1); // -1: as the current record is the last one in the list
                    temp_record = new KVPair(buckets.get(temp_bid).plaintext_kv_pairs_for_multimap.get(rand_idx).key, buckets.get(temp_bid).plaintext_kv_pairs_for_multimap.get(rand_idx).value);
                    buckets.get(temp_bid).plaintext_kv_pairs_for_multimap.remove(rand_idx);
                    eviction_counter+= 1;
                    temp_bid = get_the_smallest_bucket_id_for_a_record(temp_record.key, fanout, buckets, hashed_bucket_ids);
                    buckets.get(temp_bid).plaintext_kv_pairs_for_multimap.add(new KVPair(temp_record.key, temp_record.value));
                    if (buckets.get(temp_bid).binLoad < this.bucket_size) { // if the evicted record can be placed in the new bucket, then place it there
                        buckets.get(temp_bid).binLoad++;
                        is_finished = true;
                        break;
                    }
                }
                if(!is_finished) {
                    if (buckets.get(temp_bid).binLoad < this.bucket_size) {
                        buckets.get(temp_bid).binLoad++;
                        buckets.get(temp_bid).plaintext_kv_pairs_for_multimap.add(temp_record);
                    } else {
                        stash.add(temp_record);
                    }
                }
            }
        }
        for(int i = 0; i < buckets.size(); i++) {
            for(KVPair temp_kv: buckets.get(i).plaintext_kv_pairs_for_multimap) {
                buckets.get(i).enc_kv_pairs_for_multimap.add(new KVPair(r.nextInt(), AES.concatAndEncrypt(temp_kv.key, temp_kv.value)));
            }
        }
        //store the stash
        DataOutsourcing dataOutsourcing = new DataOutsourcing(ope);
        dataOutsourcing.storeLocalStash(LOCAL_STASH_TABLE_NAME, stash);

        return buckets;
    }

    /**
     * initialize buckets
     */
    private ArrayList<Bucket> create_empty_buckets(int total_bucket_num) {
        ArrayList<Bucket> new_bucket_list = new ArrayList<>();
        for(int bin_id = 0; bin_id < total_bucket_num; bin_id++) {new_bucket_list.add(new Bucket(bin_id));}
        return new_bucket_list;
    }

    /**
     * Cuckoo hash based approach
     */
    public int dprfMM(Map<String, List<String>> dataset, String tableName, double alpha) {
        int datasize = this.getDatasize(dataset);
        int tablesize = (int)Math.ceil(datasize * (1 + alpha));
        Map<Integer, String> hashTable1 = new HashMap<>();
        Map<Integer, String> hashTable2 = new HashMap<>();
        int pos;
        ArrayList<KVPair> stash = new ArrayList<>();
        int eviction_num = 5 * (int) Math.ceil(Math.log(datasize) / Math.log(2.0));
        String temp_record = "";
        String temp_Key, temp_Value, temp_counter;
        String[] split_res;
        int eviction_counter;

        for(String key: dataset.keySet()) {
            for (int i = 0; i < dataset.get(key).size(); i++) {
                temp_Key = key;
                temp_Value = dataset.get(key).get(i);
                temp_counter = i + "";
                for (eviction_counter = 0; eviction_counter < eviction_num; eviction_counter++) {
                    pos = computeHashPosition(temp_counter + "_" + temp_Key, tablesize);
                    if (!hashTable1.containsKey(pos)) { // if the cell is not occupied in table 1, then place the record there
                        hashTable1.put(pos, temp_Key + "_" + temp_Value + "_" + temp_counter);
                        temp_record = "";
                        break;
                    } else {
                        temp_record = hashTable1.get(pos); // get the record that has taken the cell
                        eviction_counter++;
                        hashTable1.put(pos, temp_Key + "_" + temp_Value + "_" + temp_counter);
                        split_res = temp_record.split("_");
                        temp_Key = split_res[0];
                        temp_Value = split_res[1];
                        temp_counter = split_res[2];

                        pos = computeHashPosition(temp_Key + "_" + temp_counter, tablesize);
                        if (!hashTable2.containsKey(pos)) {
                            hashTable2.put(pos, temp_Key + "_" + temp_Value + "_" + temp_counter);
                            temp_record = "";
                            break;
                        } else {
                            eviction_counter++;
                            temp_record = hashTable2.get(pos);
                            hashTable2.put(pos, temp_Key + "_" + temp_Value + "_" + temp_counter);
                            split_res = temp_record.split("_");
                            temp_Key = split_res[0];
                            temp_Value = split_res[1];
                            temp_counter = split_res[2];
                        }
                    }
                }

                if(!temp_record.equals("")){
                    split_res = temp_record.split("_");
                    temp_Key = split_res[0];
                    temp_Value = split_res[1];
                    stash.add(new KVPair(temp_Key, temp_Value));
                }
            }
        }

        ArrayList<KVPair> kvPairs_table1 = new ArrayList<>();
        ArrayList<KVPair> kvPairs_table2 = new ArrayList<>();
        String fake_value = "fake_value";
        Random r = new Random();
        int random_num;
        for(int i = 0; i < tablesize; i++) {
            if(hashTable1.containsKey(i)) {
                kvPairs_table1.add(new KVPair(AES.encrypt(i + ""), AES.concatAndEncrypt(hashTable1.get(i).split("_")[0], hashTable1.get(i).split("_")[1])));
            } else {
                random_num = r.nextInt();
                kvPairs_table1.add(new KVPair(AES.encrypt(i + ""), AES.concatAndEncrypt(fake_value + random_num)));
            }
            if(hashTable2.containsKey(i)) {
                kvPairs_table2.add(new KVPair(AES.encrypt(i + ""), AES.concatAndEncrypt(hashTable2.get(i).split("_")[0], hashTable2.get(i).split("_")[1])));
            }else {
                random_num = r.nextInt();
                kvPairs_table2.add(new KVPair(AES.encrypt(i + ""),  AES.concatAndEncrypt(fake_value + random_num)));
            }
        }

        this.outsource_dprfMM(kvPairs_table1, tableName + "_" + 1);
        this.outsource_dprfMM(kvPairs_table2, tableName + "_" + 2);

        DataOutsourcing dataOutsourcing = new DataOutsourcing(ope);
        stash.add(new KVPair(LOCAL_MAP_META_FOR_TABLESIZE_0_3, String.valueOf((int)Math.ceil(datasize * 1.3))));
        stash.add(new KVPair(LOCAL_MAP_META_FOR_TABLESIZE_0, String.valueOf((int)Math.ceil(datasize))));
        stash.add(new KVPair(LOCAL_MAP_META_FOR_LMAX, String.valueOf(this.get_max_keysize(dataset))));
        dataOutsourcing.store_multi_map(LOCAL_MAP_TABLE_DPRFMM, stash);

        return 0;
    }

    public int computeHashPosition(String str, int totalNum) {
        BigInteger bigInt = getHashBigInteger(str);
        return Math.abs(bigInt.intValue() % totalNum);
    }

    private BigInteger getHashBigInteger(String str) {
        byte[] hash = getHashBytes(str);
        return new BigInteger(1, hash);
    }

    public static byte[] getHashBytes(String str) {
        MessageDigest msgDigest;
        try {
            msgDigest = MessageDigest.getInstance("SHA-256");
        }catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        msgDigest.reset();
        return  msgDigest.digest(str.getBytes());
    }

    /**
     * query a key using multimap
     */
    public ArrayList<String> query_a_key_multimap(String queryKey, String tableName, String index_table_name,
                                                  int fanout, int total_bucket_num) {
        ArrayList<String> query_tokens_index_table = new ArrayList<>();
        for(int i = 0; i < fanout; i++) {
            query_tokens_index_table.add(this.computeHashPosition(queryKey + "_" + i, total_bucket_num) + "");
        }
        ArrayList<String> multimaps = this.dq.get_values_using_string_tokens(index_table_name, KEY_COLUMN, VALUE_COLUMN, query_tokens_index_table);
        ArrayList<String> query_tokens_data_table = new ArrayList<>();
        for(String multimap: multimaps) {
            String[] tokens = multimap.split("_");
            query_tokens_data_table.addAll(Arrays.asList(tokens));
        }
        ArrayList<String> encBins = this.dq.get_values_using_string_tokens(tableName, "key", "value", query_tokens_data_table);

        return this.dq.get_results_from_encrypted_strings(queryKey, encBins);
    }

    /**
     * query a key using the cuckoo-hash based approach
     */
    public ArrayList<String> query_a_key_from_dprfMM(String queryKey, String tableName, int Lmax, int tablesize) {
        ArrayList<String> query_tokens_table1 = new ArrayList<>();
        ArrayList<String> query_tokens_table2 = new ArrayList<>();
        for(int i = 0; i < Lmax; i++) {
            int pos = computeHashPosition(i + "_" + queryKey, tablesize);
            query_tokens_table1.add(AES.encrypt(pos + ""));
            int pos2 = computeHashPosition(queryKey + "_" + i, tablesize);
            query_tokens_table2.add(AES.encrypt(pos2 + ""));
        }
        ArrayList<String> results = this.dq.get_values_using_string_tokens(tableName + "_" + 1, "key", "value", query_tokens_table1);
        ArrayList<String> results_from_table2 = this.dq.get_values_using_string_tokens(tableName + "_" + 2, "key", "value", query_tokens_table2);
        results.addAll(results_from_table2);
        return this.dq.get_results_from_encrypted_strings(queryKey, results);
    }

    /**
     * compute Lmax
     */
    public int get_max_keysize(Map<String, List<String>> dataset) {
        int max_key_size = -1;
        for(String key: dataset.keySet()){
            if(dataset.get(key).size() > max_key_size) {
                max_key_size = dataset.get(key).size();
            }
        }
        return max_key_size;
    }

}



