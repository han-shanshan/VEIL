package DataProcessing.XorMM;
import DBOperation.PostgreSQLOperation.DBOperator;
import DataProcessing.DataQuery;
import java.util.ArrayList;
import java.util.Arrays;
import static DataProcessing.XorMM.XorMMUtil.TtS;
import static DataProcessing.XorMM.XorMMUtil.Xor_XorMM;

public class XorMMServer {
    private static int MAX_VOLUME_LENGTH;
    private static int server_level;
    private static int server_DEFAULT_INITIAL_CAPACITY;
    private DataQuery dq;
    private String tableName;
    private String keyField;
    private String valField;
    private ArrayList<byte[]> C_key = new ArrayList<byte[]>();


    public XorMMServer(int volume_length, int level, int DEFAULT_INITIAL_CAPACITY, String dbName, String tableName, String keyField, String valField){
        MAX_VOLUME_LENGTH = volume_length;
        server_level = level;
        server_DEFAULT_INITIAL_CAPACITY = DEFAULT_INITIAL_CAPACITY;
        dq = new DataQuery(new DBOperator(dbName));
        this.tableName = tableName;
        this.keyField = keyField;
        this.valField = valField;
    }

    public void  Query_Xor(byte[] hash){
        for (int i = 0;i<MAX_VOLUME_LENGTH;i++ ) {
            GGM.clear();
            ArrayList<Integer> queryTokens = new ArrayList<>();
            byte[] father_Node = GGM.Tri_GGM_Path(hash, server_level, TtS(i, 3, server_level));
            int t0 = GGM.Map2Range(Arrays.copyOfRange(father_Node, 1 , 9),server_DEFAULT_INITIAL_CAPACITY,0);
            int t1 = GGM.Map2Range(Arrays.copyOfRange(father_Node, 11, 19),server_DEFAULT_INITIAL_CAPACITY,1);
            int t2 = GGM.Map2Range(Arrays.copyOfRange(father_Node, 21, 29),server_DEFAULT_INITIAL_CAPACITY,2);
            queryTokens.add(t0);
            queryTokens.add(t1);
            queryTokens.add(t2);
            ArrayList<byte[]> emm_results = this.dq.get_values_using_integer_tokens_xormm(this.tableName, keyField, valField, queryTokens);
            byte[] res = Xor_XorMM(Xor_XorMM(emm_results.get(0), emm_results.get(1)), emm_results.get(2));
            C_key.add(res);
        }
    }

    public ArrayList<byte[]> Get_C_key(){ return C_key; }
    public void Clear(){ C_key.clear();}
}
