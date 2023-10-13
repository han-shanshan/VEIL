package Modules;
import java.util.*;

public class Bucket {
    public int binID;
    public int binLoad = 0;
    public int borrowing_bin_num = 0;
    public ArrayList<String> keyArray;
    public List<KVPair> enc_kv_pairs_for_multimap = new ArrayList<>();
    public List<KVPair> plaintext_kv_pairs_for_multimap = new ArrayList<>();

    public Bucket(){
        this.binLoad = 0;
        this.keyArray = new ArrayList<>();
    }

    public Bucket(int bucket_id) {
        this.binLoad = 0;
        this.keyArray = new ArrayList<>();
        this.binID = bucket_id;
        this.borrowing_bin_num = 0;
    }

    public Bucket(String key, int load) {
        this.binLoad = load;
        this.keyArray = new ArrayList<>();
        this.keyArray.add(key);
    }

    public Bucket(Bucket b) {
        this.keyArray = new ArrayList<>();
        copy_a_bin(b);
    }

    public void copy_a_bin(Bucket b) {
        this.keyArray.addAll(b.keyArray);
        this.binLoad = b.binLoad;
        this.binID = b.binID;
        this.borrowing_bin_num = b.borrowing_bin_num;
    }
}
