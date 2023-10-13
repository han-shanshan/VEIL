package DataProcessing.XorMM;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static DataProcessing.XorMM.XorMMUtil.*;

public class GGM {
    public static Map<Long,byte[]> map = new HashMap<Long,byte[]>();

    public static void clear(){
        map.clear();
    }

    public static byte[] Tri_GGM_Path(byte[] root, int level,int path[]){
        byte[] current_node = root;
        for(int i=0;i<level;i++){
            int temp = path[i];
            if(temp==0) {
                current_node = Arrays.copyOfRange(current_node, 1, 10);
                long key = hash64(bytesToLong(current_node),temp);
                if(map.containsKey(key)){
                    current_node = map.get(key);
                }else{
                    current_node = Get_SHA_256(current_node);
                    map.put(key,current_node);
                }
            }else if(temp==1){
                current_node = Arrays.copyOfRange(current_node, 11, 20);
                long key = hash64(bytesToLong(current_node),temp);
                if(map.containsKey(key)){
                    current_node = map.get(key);
                }else{
                    current_node = Get_SHA_256(current_node);
                    map.put(key,current_node);
                }
            }else {
                current_node = Arrays.copyOfRange(current_node, 21, 30);
                long key = hash64(bytesToLong(current_node),temp);
                if(map.containsKey(key)){
                    current_node = map.get(key);
                }else{
                    current_node = Get_SHA_256(current_node);
                    map.put(key,current_node);
                }
            }
        }
        return current_node;
    }

    public static int Map2Range(byte[] hash,int capacity,int index) {
        long r = bytesToLong(hash);
        r = reduce((int) r,  capacity);
        r = r + (long) index * capacity;
        return (int) r;
    }



}
