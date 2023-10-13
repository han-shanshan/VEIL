package Utils;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class AES {
    private static SecretKeySpec secretKey;
    private static byte[] key = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};

    public static void setKey()
    {
        try {
            secretKey = new SecretKeySpec(key, "AES");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String encrypt(String strToEncrypt) {
        try {
            setKey();
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            return Base64.getEncoder()
                    .encodeToString(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
        } catch (Exception e) {
            System.out.println("Error while encrypting: " + e.toString());
        }
        return null;
    }

    public static String decrypt( String strToDecrypt) {
        try {
            setKey();
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            return new String(cipher.doFinal(Base64.getDecoder()
                    .decode(strToDecrypt)));
        } catch (Exception e) {
            System.out.println("Error while decrypting: " + e.toString());
        }
        return null;
    }

    public static String concatAndEncrypt(String ...strsToEncrypt){
        String concatSeperator = "||";
        String concatRawStr = String.join(concatSeperator, strsToEncrypt);

        return encrypt(concatRawStr);
    }
}