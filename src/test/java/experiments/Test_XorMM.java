package experiments;
import DataProcessing.XorMM.XorMMServer;
import DataProcessing.XorMM.Xor_Hash;
import Modules.KVPair;
import Utils.AES;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static DataProcessing.XorMM.XorMMUtil.*;
import static Utils.Constant.*;

public class Test_XorMM extends Test_BaseClass {
    int beta = 0;//parameter for xor hash // set different value of beta
    private String dbName;

    public Test_XorMM(String dbName)  {
        super(dbName);
        this.dbName = dbName;
    }

    public void XorMM_test(String tableName, String keyField, String valField){
        Map<String, List<String>> dataset = owner.dq.read_a_dataset(tableName, keyField, valField);
        long bucket_creation_start = System.currentTimeMillis();
        //maximum volume length
        int MAX_VOLUME_LENGTH = this.owner.get_max_keysize(dataset);
        int XOR_LEVEL = (int) Math.ceil(Math.log(MAX_VOLUME_LENGTH) / Math.log(3.0));//GGM Tree level for xor hash
        int ELEMENT_SIZE = owner.getDatasize(dataset);

        //storage size
        KVPair[] kv_list = new KVPair[ELEMENT_SIZE];
        int counter = 0;
        for (String key: dataset.keySet()) {
            int keysize = dataset.get(key).size(); //get the key size
            for(int j = 0; j < keysize; j++) {
                kv_list[counter] = new KVPair(key, dataset.get(key).get(j), j);
                counter ++;
            }
        }

        Xor_Hash xor = new Xor_Hash(beta);
        xor.XorMM_setup(kv_list, XOR_LEVEL);
        owner.outsource_dataset_xormm(xor.Get_EMM(), ENC_XORMM_TABLE_NAME);
    }

    private void testQueries_xormm(String source_table, String datasource_keyField, String datasource_valField) {
        Map<String, List<String>> dataset = owner.dq.read_a_dataset(source_table, datasource_keyField, datasource_valField);
        //maximum volume length
        int MAX_VOLUME_LENGTH = this.owner.get_max_keysize(dataset);
        int XOR_LEVEL = (int) Math.ceil(Math.log(MAX_VOLUME_LENGTH) / Math.log(3.0));//GGM Tree level for xor hash
        int ELEMENT_SIZE = owner.getDatasize(dataset);
        int STORAGE_XOR = (int) Math.floor(((ELEMENT_SIZE * 1.23) + beta) / 3);
        Xor_Hash xor = new Xor_Hash(beta);
        long K_d = xor.Get_K_d();
        int K_e = xor.Get_K_e();

        XorMMServer xor_server = new XorMMServer(MAX_VOLUME_LENGTH, XOR_LEVEL, STORAGE_XOR, this.dbName, ENC_XORMM_TABLE_NAME, KEY_COLUMN, VALUE_COLUMN);//server receives ciphertext
        int keyNum = 20;
        String[] queryKeys = new String[keyNum];
        Random r = new Random();
        ArrayList<String> allKeys = owner.dq.query_for_distinct_keys_from_lineitem_table();
        for(int i = 0; i < keyNum; i++) {
            queryKeys[i] = allKeys.get(r.nextInt(allKeys.size()));
        }

        for (String search_key : queryKeys) {
            //query phase
            byte[] tk_key = Get_SHA_256((search_key + K_d).getBytes(StandardCharsets.UTF_8));//search token
            xor_server.Query_Xor(tk_key);//search
            ArrayList<byte[]> C_key = xor_server.Get_C_key();//client receives results
            byte[] K = Get_Sha_128((K_e + search_key).getBytes(StandardCharsets.UTF_8));

            for (int i = 0; i < C_key.size(); i++)//decryption
            {
                byte[] str_0 = new byte[0];
                try {
                    str_0 = XorMM_decrypt(K, C_key.get(i));
                } catch (NoSuchPaddingException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchAlgorithmException|InvalidKeyException
                         |BadPaddingException|IllegalBlockSizeException |InvalidAlgorithmParameterException e) {
                    System.out.println(e);
                }
                if (str_0 != null) {
                    String s = new String(str_0);
                    System.out.print(s + "  ");
                }
            }
            xor_server.Clear();
        }
    }

    public static void main(String[] args) {
        String[] dbNames = new String[]{"tpch_6mz0_4"};
        for(String dbName: dbNames) {
            new Test_XorMM(dbName).XorMM_test("LINEITEM", "L_PARTKEY", "ALL");
            new Test_XorMM(dbName).testQueries_xormm("LINEITEM", "L_PARTKEY", "ALL");
        }
    }
}
