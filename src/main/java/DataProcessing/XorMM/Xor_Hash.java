package DataProcessing.XorMM;
import Modules.KVPair;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import static DataProcessing.XorMM.XorMMUtil.*;


public class Xor_Hash {
    private static Random random = new Random();
    private static int K_e = 012;
    private static long K_d = 123;
    private int beta;
    static int Try_Times;
    static byte[][] enc_list;
    static byte[][] EMM;
    private static Map<String,byte[]> k_list = new HashMap<String,byte[]>();
    private static Map<String,Integer> leave_map = new HashMap<String,Integer>();

    public Xor_Hash(int new_beta){
        beta = new_beta;
    }

    //the setup algorithm for XorMM scheme
    public void XorMM_setup(KVPair[] kv_list, int level) {
        int table_size = (int) Math.floor(((kv_list.length*1.23)+beta)/3);
        EMM = new byte[table_size*3][];
        enc_list = new byte[kv_list.length][];

        for (int i = 0; i < kv_list.length; i++) {
            byte[] K;
            if(k_list.containsKey(kv_list[i].key))
                K = k_list.get(kv_list[i].key);
            else {
                K = Get_Sha_128((K_e+kv_list[i].key).getBytes());
                k_list.put(kv_list[i].key,K);
            }
            enc_list[i] = XorMMUtil.XorMM_encrypt(K,(kv_list[i].value).getBytes());
        }
        MappingStep(kv_list,table_size,level);
        for(int i=0;i<EMM.length;i++){
            if(EMM[i]==null){
                EMM[i] = Get_Sha_128(longToBytes(random.nextInt(1000)));
            }
        }
    }

    void MappingStep(KVPair[] kv_list,int table_size, int level) {
        int arrayLength = table_size * 3;
        long[] reverseOrder = new long[arrayLength];
        byte[] reverseH = new byte[arrayLength];
        int HASHES = 3;
        int reverseOrderPos;

        do {
            reverseOrderPos = 0;
            leave_map.clear();
//            K_d = random.nextLong(); // remove this since we want to fix K_d
            byte[] t2count = new byte[arrayLength];
            long[] t2 = new long[arrayLength];
            for (int i = 0; i < kv_list.length; i++) {
                long k = i;
                for (int hi = 0; hi < HASHES; hi++) {
                    String ks = kv_list[(int)k].key+","+kv_list[(int)k].XorMM_counter;
                    String k0 = ks+","+hi;
                    int Node,current;
                    if(leave_map.containsKey(k0)) {
                        current = leave_map.get(k0);
                    }else {
                        GGM.clear();
                        byte[] kv = GGM.Tri_GGM_Path(XorMMUtil.Get_SHA_256((kv_list[(int) k].key+K_d).getBytes()), level, XorMMUtil.TtS(kv_list[(int) k].XorMM_counter, 3, level));
                        current = GGM.Map2Range(Arrays.copyOfRange(kv, 1 , 9),table_size,0);
                        leave_map.put(k0,current);
                        Node = GGM.Map2Range(Arrays.copyOfRange(kv, 11, 19),table_size,1);
                        leave_map.put(ks+",1",Node);
                        Node = GGM.Map2Range(Arrays.copyOfRange(kv, 21, 29),table_size,2);
                        leave_map.put(ks+",2",Node);
                    }
                    int h = current;
                    t2[h]^=  k;
                    if (t2count[h] > 120) {
                        throw new IllegalArgumentException();
                    }
                    t2count[h]++;
                }
            }
            int[][] alone = new int[HASHES][table_size];
            int[] alonePos = new int[HASHES];
            for (int nextAlone = 0; nextAlone < HASHES; nextAlone++) {
                for (int i = 0; i < table_size; i++) {
                    if (t2count[nextAlone * table_size + i] == 1) {
                        alone[nextAlone][alonePos[nextAlone]++] = nextAlone * table_size + i;
                    }
                }
            }
            int found = -1;
            while (true) {
                int i = -1;
                for (int hi = 0; hi < HASHES; hi++) {
                    if (alonePos[hi] > 0) {
                        i = alone[hi][--alonePos[hi]];
                        found = hi;
                        break;
                    }
                }
                if (i == -1) {
                    break;
                }
                if (t2count[i] <= 0) {
                    continue;
                }
                long k = t2[i];
                if (t2count[i] != 1) {
                    throw new AssertionError();
                }
                --t2count[i];
                for (int hi = 0; hi < HASHES; hi++) {
                    if (hi != found) {
                        int h = leave_map.get(kv_list[(int)k].key+","+kv_list[(int)k].XorMM_counter+","+hi);
                        int newCount = --t2count[h];
                        if (newCount == 1) {
                            alone[hi][alonePos[hi]++] = h;
                        }
                        t2[h]^= k;
                    }
                }
                reverseOrder[reverseOrderPos] = k;
                reverseH[reverseOrderPos] = (byte) found;
                reverseOrderPos++;
            }
            Try_Times++;
        }while(reverseOrderPos  != kv_list.length);
        for (int i = reverseOrderPos - 1; i >= 0; i--) {
            int k = (int) reverseOrder[i];
            int found = reverseH[i];
            int change = -1;
            byte[] xor = enc_list[k];
            for (int hi = 0; hi < HASHES; hi++) {
                int h = leave_map.get(kv_list[(int)k].key+","+kv_list[(int)k].XorMM_counter+","+hi);
                if (found == hi) {
                    change = h;
                } else {
                    if(EMM[h]==null) {
                        EMM[h] = XorMMUtil.Get_Sha_128(longToBytes(random.nextInt(10000)));
                    }
                    xor = Xor_XorMM(xor, EMM[h]);
                }
            }
            EMM[change] =  xor;
        }
    }

    public long Get_K_d(){
        return K_d;
    }

    public int Get_K_e() { return K_e; }

    public byte[][] Get_EMM(){ return EMM;}
}
