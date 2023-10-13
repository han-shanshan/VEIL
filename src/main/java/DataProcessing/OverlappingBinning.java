package DataProcessing;
import Modules.Bucket;
import Modules.KVPair;
import Utils.AES;
import java.util.*;

public class OverlappingBinning {
    public int bucket_size, alpha;
    public OverlappingBinning(int bucket_size) {
        this.bucket_size = bucket_size;
    }

    /**
     * sort buckets based on their sizes
     */
    HashMap<Integer, Integer> sortBucketList(ArrayList<Bucket> bins){
        ArrayList<Bucket> newBins = new ArrayList<>();
        for(int i = 0; i < bins.size(); i++) {
            Bucket b = new Bucket();
            b.copy_a_bin(bins.get(i));
            newBins.add(b);
        }
        Collections.sort(newBins, new Comparator<Bucket>() {
            @Override
            public int compare(Bucket b1, Bucket b2) {
                return b1.binLoad - b2.binLoad;
            }
        });
        HashMap<Integer, Integer> orderBinMap = new HashMap<>();
        for(int i = 0; i < newBins.size(); i++) {
            orderBinMap.put(i, newBins.get(i).binID);
        }
        newBins.clear();
        return orderBinMap;
    }

    /**
     * get the smallest bucket size
     */
    int getSmallestBucketSize(ArrayList<Bucket> buckets, int bucketSize) {
        int smallest_bucket_size = bucketSize;
        for(int i = 0; i < buckets.size(); i++) {
            if(buckets.get(i).binLoad < smallest_bucket_size) {
                smallest_bucket_size = buckets.get(i).binLoad;
            }
        }
        return smallest_bucket_size;
    }

    /**
     * create a d-regular graph using the created bucekts
     */
    public ArrayList<KVPair> create_d_regular_graph_multimap(ArrayList<Bucket> buckets, int degree, int cluster_idx) {
        int bucket_num_in_cluster = buckets.size();
        if(buckets.size() * degree % 2 != 0) {
            System.out.println("can not construct a single cluster with degree = " + degree);
            return null;
        }
        ArrayList<Integer> functions = getFunctions(buckets.size(), degree);
        HashMap<Integer, Integer> binOrderMap = sortBucketList(buckets);
        //store map buckets: <giver_bucket_id->receiver_bucket_id, function_id>
        ArrayList<KVPair> multi_map = new ArrayList<>();
        int overlapping_size = getInitialOverlapping_size(buckets, degree, cluster_idx, bucket_num_in_cluster, functions, binOrderMap);
        int current_bin_id, idx;
        HashMap<String, Integer> binMap = new HashMap<>();

        for(int i = 0; i < buckets.size(); i++) {
            current_bin_id = binOrderMap.get(i);
            ArrayList<Integer> tempBinList = new ArrayList<>();
            current_bin_id = current_bin_id - cluster_idx * bucket_num_in_cluster;

            for(int j = 0; j < functions.size(); j++) {
                idx = (functions.get(j) + current_bin_id + buckets.size()) % buckets.size();
                tempBinList.add(idx);
            }
            Collections.sort(tempBinList, new Comparator<Integer>() { // decreasing
                @Override
                public int compare(Integer b1, Integer b2) {
                    return buckets.get(b2).binLoad - buckets.get(b1).binLoad;
                }
            });

            overlapping_size = get_adjusted_overlapping_size(buckets, overlapping_size, current_bin_id);
            int binLoad = compute_bucket_load(buckets.get(current_bin_id), overlapping_size);
            int function_id;
            for (int k = 0; k < tempBinList.size(); k++) {
                if (!binMap.containsKey(current_bin_id + "->" + tempBinList.get(k)) &&
                        !binMap.containsKey(tempBinList.get(k) + "->" + current_bin_id)) {
//                  compute function id to indicate which values to retrieve when borrowing/lending
                    if(k == tempBinList.size() - 1 || k % 2 == 0) {
                        function_id = k;
                    } else {function_id = k-1;}

                    if (binLoad + overlapping_size < this.bucket_size) {
                        binMap.put(tempBinList.get(k) + "->" + current_bin_id, function_id);
                        binLoad += overlapping_size;
                        // use borrowing_bin_num to indicate how many neighbors that lend values to it
                        // do not update bin.binload as the overlapping size may change
                        buckets.get(current_bin_id).borrowing_bin_num += 1;
                    } else {
                        int tempBinLoad = compute_bucket_load(buckets.get(tempBinList.get(k)), overlapping_size);
                        // if its neighbor can not take more values, then reduce overlapping size
                        if (tempBinLoad + overlapping_size > bucket_size) {
                            binMap.put(tempBinList.get(k) + "->" + current_bin_id, function_id);
                            buckets.get(current_bin_id).borrowing_bin_num += 1;
                            overlapping_size = (this.bucket_size - buckets.get(current_bin_id).binLoad) / buckets.get(current_bin_id).borrowing_bin_num;
                            binLoad = buckets.get(current_bin_id).binLoad + overlapping_size * buckets.get(current_bin_id).borrowing_bin_num;
                        } else { // if its neighbor can take more values, then set the neighbor bucket to be receiver
                            binMap.put(current_bin_id + "->" + tempBinList.get(k), function_id);
                            buckets.get(tempBinList.get(k)).borrowing_bin_num += 1;
                        }
                    }
                }
            }
        }

        // Process binMap
        HashMap<String, ArrayList<String>> receiver_giver_map = process_bucket_map(binMap);
        for(String k: binMap.keySet()){
            String receiver = k.split("->")[1];
            String giver = k.split("->")[0];
            if(!receiver_giver_map.containsKey(receiver)) {
                receiver_giver_map.put(receiver, new ArrayList<>());
            }
            receiver_giver_map.get(receiver).add(giver + "_" + binMap.get(k));
        }

        //padding
        String fake_key = "fake_key", fake_value = "fake_value";
        Random r = new Random();
        int random_num, fake_num;
        for (int i = 0; i < buckets.size(); i++) {
            fake_num = this.bucket_size - buckets.get(i).enc_kv_pairs_for_multimap.size() - buckets.get(i).borrowing_bin_num * overlapping_size;
            for (int j2 = 0; j2 < fake_num; j2++) {
                random_num = r.nextInt();
                buckets.get(i).enc_kv_pairs_for_multimap.add(new KVPair(r.nextInt(), AES.concatAndEncrypt(fake_key + random_num, fake_value + random_num)));
            }
        }

        for(Bucket b: buckets) {
            StringBuilder temp_meta = new StringBuilder();
            for(KVPair kv: b.enc_kv_pairs_for_multimap) {
                temp_meta.append(kv.key).append("_");
            }

            if(b.borrowing_bin_num > 0) {
                for(String bucket_and_function_id: receiver_giver_map.get(b.binID + "")) {
                    String[] temp = bucket_and_function_id.split("_"); //todo: check not contain the index
                    idx = overlapping_size * Integer.parseInt(temp[1]);  //temp[1]: function id
                    for(int i = 0; i < overlapping_size; i++) {
                        temp_meta.append(buckets.get(Integer.parseInt(temp[0])).enc_kv_pairs_for_multimap.get(idx+i).key).append("_");
                    }
                }
            }
            multi_map.add(new KVPair(b.binID, temp_meta.toString()));
        }

        return multi_map;
    }


    public ArrayList<KVPair> create_a_d_regular_graph_with_given_desired_overlapping_size(ArrayList<Bucket> buckets, int degree, int desired_overlapping_size) {
        if(buckets.size() * degree % 2 != 0) {
            System.out.println("can not construct a single cluster with degree = " + degree);
            return null;
        }
        ArrayList<Integer> functions = getFunctions(buckets.size(), degree);
        HashMap<Integer, Integer> binOrderMap = sortBucketList(buckets);
        ArrayList<KVPair> multi_map = new ArrayList<>(); //store map buckets: <giver_bucket_id->receiver_bucket_id, function_id>
        int current_bin_id, idx;
        HashMap<String, Integer> binMap = new HashMap<>();
        int additional_stash_size = 0;
        if(bucket_size/degree < desired_overlapping_size) {
            desired_overlapping_size = bucket_size/degree - 1;
        }

        for(int i = 0; i < buckets.size(); i++) {
            current_bin_id = binOrderMap.get(i);
            ArrayList<Integer> tempBinList = new ArrayList<>(); // getTempBucketList(buckets, functions, current_bin_id);
            for(int j = 0; j < functions.size(); j++) {
                idx = (functions.get(j) + current_bin_id + buckets.size()) % buckets.size(); //- cluster_idx * bucket_num_in_cluster;
                tempBinList.add(idx);
            }
            Collections.sort(tempBinList, new Comparator<Integer>() { // decreasing
                @Override
                public int compare(Integer b1, Integer b2) {
                    return buckets.get(b2).binLoad - buckets.get(b1).binLoad;
                }
            });
            int binLoad = compute_bucket_load(buckets.get(current_bin_id), desired_overlapping_size);
            int function_id;
            for (int k = 0; k < tempBinList.size(); k++) {
                if (!binMap.containsKey(current_bin_id + "->" + tempBinList.get(k)) &&
                        !binMap.containsKey(tempBinList.get(k) + "->" + current_bin_id)) {
//                  compute function id to indicate which values to retrieve when borrowing/lending
                    if(k == tempBinList.size() - 1 || k % 2 == 0) {
                        function_id = k;
                    } else {function_id = k-1;}

                    if (binLoad + desired_overlapping_size < this.bucket_size) {
                        binMap.put(tempBinList.get(k) + "->" + current_bin_id, function_id);
                        binLoad += desired_overlapping_size;
                        // use borrowing_bin_num to indicate how many neighbors that lend values to it
                        // do not update bin.binload as the overlapping size may change
                        buckets.get(current_bin_id).borrowing_bin_num += 1;
                    } else {
                        int tempBinLoad = compute_bucket_load(buckets.get(tempBinList.get(k)), desired_overlapping_size);
                        // if its neighbor can not take more values, then reduce overlapping size
                        if (tempBinLoad + desired_overlapping_size > bucket_size) {
                            binMap.put(tempBinList.get(k) + "->" + current_bin_id, function_id);
                            buckets.get(current_bin_id).borrowing_bin_num += 1;
                            for(int x = 0; x < binLoad + desired_overlapping_size - bucket_size; x++) {
                                buckets.get(current_bin_id).enc_kv_pairs_for_multimap.remove(0);
                                additional_stash_size += 1;
                            }
                            binLoad = bucket_size;
                        } else { // if its neighbor can take more values, then set the neighbor bucket to be receiver
                            binMap.put(current_bin_id + "->" + tempBinList.get(k), function_id);
                            buckets.get(tempBinList.get(k)).borrowing_bin_num += 1;
                        }
                    }
                }
            }
        }
        // Process binMap
        HashMap<String, ArrayList<String>> receiver_giver_map = process_bucket_map(binMap);

        for(String k: binMap.keySet()){
            String receiver = k.split("->")[1];
            String giver = k.split("->")[0];
            if(!receiver_giver_map.containsKey(receiver)) {
                receiver_giver_map.put(receiver, new ArrayList<>());
            }
            receiver_giver_map.get(receiver).add(giver + "_" + binMap.get(k));
        }

        // padding
        String fake_key = "fake_key", fake_value = "fake_value";
        Random r = new Random();
        int random_num, fake_num;
        for (int i = 0; i < buckets.size(); i++) {
            fake_num = this.bucket_size - buckets.get(i).enc_kv_pairs_for_multimap.size() - buckets.get(i).borrowing_bin_num * desired_overlapping_size;
            for (int j2 = 0; j2 < fake_num; j2++) {
                random_num = r.nextInt();
                buckets.get(i).enc_kv_pairs_for_multimap.add(new KVPair(r.nextInt(), AES.concatAndEncrypt(fake_key + random_num, fake_value + random_num)));
            }
        }

        for(Bucket b: buckets) {
            StringBuilder temp_meta = new StringBuilder();
            for(KVPair kv: b.enc_kv_pairs_for_multimap) {
                temp_meta.append(kv.key).append("_");
            }
            int b_load = b.enc_kv_pairs_for_multimap.size();
            int borrow_num;
            if(b.borrowing_bin_num > 0) {
                for(String bucket_and_function_id: receiver_giver_map.get(b.binID + "")) {
                    String[] temp = bucket_and_function_id.split("_"); //todo: check not contain the index
                    idx = desired_overlapping_size * Integer.parseInt(temp[1]);  //temp[1]: function id
                    borrow_num = desired_overlapping_size;
                    if(b_load + desired_overlapping_size > bucket_size) {
                        borrow_num = bucket_size - b_load;
                    }
                    for(int i = 0; i < borrow_num; i++) {
                        temp_meta.append(buckets.get(Integer.parseInt(temp[0])).enc_kv_pairs_for_multimap.get(idx+i).key).append("_");
                    }
                }
            }
            multi_map.add(new KVPair(b.binID, temp_meta.toString()));
        }

        return multi_map;
    }

    private HashMap<String, ArrayList<String>> process_bucket_map(HashMap<String, Integer> binMap) {
        HashMap<String, ArrayList<String>> binMap2 = new HashMap<>();
        for(String key: binMap.keySet()) {
            String[] temp = key.split("->");
            if(!binMap2.containsKey(temp[1])) {
                binMap2.put(temp[1], new ArrayList<>());
            }
            binMap2.get(temp[1]).add(temp[0] + "_" + binMap.get(key));
        }
        return binMap2;
    }

    /**
     * adjust the overlapping size
     */
    private int get_adjusted_overlapping_size(ArrayList<Bucket> bins, int overlapping_size, int current_bin_id) {
        int binLoad = bins.get(current_bin_id).binLoad;
        if(binLoad + overlapping_size * bins.get(current_bin_id).borrowing_bin_num > this.bucket_size) {
            if (bins.get(current_bin_id).borrowing_bin_num == 0) {
                overlapping_size = 0;
            } else {
                overlapping_size = (this.bucket_size - binLoad) / bins.get(current_bin_id).borrowing_bin_num;
            }
        }
        return overlapping_size;
    }

    /**
     * compute an initial value of the overlapping size
     */
    private int getInitialOverlapping_size(ArrayList<Bucket> bins, int degree, int cluster_idx, int bucket_num_in_cluster, ArrayList<Integer> functions, HashMap<Integer, Integer> binOrderMap) {
        int smallest_bin_size = this.getSmallestBucketSize(bins, this.bucket_size);
        int overlapping_size = (this.bucket_size - smallest_bin_size) / degree;
//      Step1 (use the smallest bin): (bin size - smallest bin size) / degree, overlapping size = " + overlapping_size
        if(this.bucket_size / degree < overlapping_size) {
            overlapping_size = this.bucket_size / degree;
        }
//      "Step2 (use the largest bin): bin size / degree, overlapping size = " + overlapping_size
        int idx = 0, temp_id = 0;
        for(int i = bins.size() - 1; i >= 0; i--) {
            temp_id = binOrderMap.get(i) - cluster_idx * bucket_num_in_cluster;
            if(bins.get(temp_id).binLoad == this.bucket_size) {
                for(int j = 0; j < functions.size(); j++) {
                    idx = (functions.get(j) + binOrderMap.get(temp_id) + bins.size())  % bins.size();
                    if(this.bucket_size - bins.get(idx).binLoad < overlapping_size) {
                        overlapping_size = this.bucket_size - bins.get(idx).binLoad;
                    }
                }
            } else {break; }
        }
        return overlapping_size;
    }

    /**
     * get the functions used to find neighboring buckets
     */
    public ArrayList<Integer> getFunctions(int bin_num, int degree) {
        ArrayList<Integer> functions = new ArrayList<>();
        for(int idx = 1; idx <= degree / 2; idx++) {
            functions.add(idx);
            functions.add(-idx);
        }
        if(degree % 2 == 1) {
            functions.add(bin_num / 2);
        }
        return functions;
    }

    private int compute_bucket_load(Bucket b, int overlapping_size) {
        return b.binLoad + overlapping_size * b.borrowing_bin_num;
    }
}
