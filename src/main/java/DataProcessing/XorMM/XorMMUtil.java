package DataProcessing.XorMM;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class XorMMUtil {
    public static final String InitVector = "EncDecInitVector";

    public static byte[] XorMM_encrypt(byte[] K_e,byte[] plaintext)  {
        IvParameterSpec iv = new IvParameterSpec(InitVector.getBytes(StandardCharsets.UTF_8));
        SecretKeySpec secretKeySpec = new SecretKeySpec(K_e, "AES");
        Cipher cipher = null;
        try {
            cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
            return cipher.doFinal(plaintext);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
                 | IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] XorMM_decrypt(byte[] K_e,byte[] ciphertext) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException, InvalidAlgorithmParameterException {
        SecretKeySpec secretKeySpec = new SecretKeySpec(K_e, "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
        try {
            byte[] res = cipher.doFinal(ciphertext);
            return res;
        }catch (Exception e){}
        return  null;
    }

    public static long hash64(long x, long seed) {
        x += seed;
        x = (x ^ (x >>> 33)) * 0xff51afd7ed558ccdL;
        x = (x ^ (x >>> 33)) * 0xc4ceb9fe1a85ec53L;
        x = x ^ (x >>> 33);
        return x;
    }

    public static long bytesToLong(byte[] bytes) {
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (bytes[ix] & 0xff);
        }
        return num;
    }

    public static int reduce(int hash, int n) {
        return (int) (((hash & 0xffffffffL) * n) >>> 32);
    }

    public static byte[]  Get_Sha_128(byte[] passwordToHash) {
        try {
            MessageDigest md = MessageDigest.getInstance("Sha-256");
            md.update(passwordToHash);
            byte[] bytes = md.digest();
            byte[] hash_128 = new byte[16];
            System.arraycopy(bytes,0,hash_128,0,16);
            return hash_128;
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[]  Get_SHA_256(byte[] passwordToHash) {
        try {
            MessageDigest md = MessageDigest.getInstance("Sha-256");
            md.update(passwordToHash);
            byte[] bytes = md.digest();
            return bytes;
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] longToBytes(long num) {
        byte[] byteNum = new byte[8];
        for (int ix = 0; ix < 8; ++ix) {
            int offset = 64 - (ix + 1) * 8;
            byteNum[ix] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }

    public static int[] TtS(int inNum, int index, int level) {
        int[] result = new int[level];
        int i = 0;
        while (i < level) {
            result[i] = (inNum % index);
            inNum = inNum / index;
            i++;
        }
        return result;
    }

    public static byte[] Xor_XorMM(byte[] x, byte[] y) {
        int min =0;
        if(x.length>y.length){
            min = y.length;
        }else{
            min = x.length;
        }
        byte[] temp = new byte[min];
        for (int i = 0; i < min; i++) {
            temp[i] = (byte) (x[i] ^ y[i]);
        }
        return temp;
    }
}
