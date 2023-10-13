package Modules;

public class KVPair {
    public String key;
    public String value;
    public int XorMM_counter;
    public KVPair(){}
    public KVPair(String key, String value) {
        this.key = key;
        this.value = value;
    }
    public KVPair(int binID, String value) {
        this.key = binID + "";
        this.value = value;
    }

    public KVPair(String key, String value, int xorMM_counter) {
        this.key = key;
        this.value = value;
        this.XorMM_counter = xorMM_counter;
    }
}
