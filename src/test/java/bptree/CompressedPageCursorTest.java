package bptree;

import org.junit.Test;

import java.util.Arrays;

/**
 * Created by max on 6/8/15.
 */
public class CompressedPageCursorTest {

    @Test
    public void bitTesting() {

        int keyLength = 3;

        long[] keyA = new long[]{123,321,451};
        long[] keyB = new long[]{124,322,452};
        System.out.println(Arrays.toString(keyB));

        encodeKey(keyB, keyA);

    }
    public void encodeKey(long[] key, long[] prev){

        long[] diff = new long[key.length];
        for(int i = 0; i < key.length; i++)
        {
            diff[i] = key[i] - prev[i];
        }

        int maxNumBytes = numberOfBytes(diff[0]);
        for(int i = 1; i < key.length; i++){
            maxNumBytes = Math.max(maxNumBytes, numberOfBytes(diff[i]));
        }

        byte[] encoded = new byte[1 + (maxNumBytes * 3 )];
        encoded[0] = (byte)maxNumBytes;
        for(int i = 0; i < key.length; i++){
            toBytes(diff[i], encoded, 1 + (i * maxNumBytes), maxNumBytes);
        }

        System.out.println("Original KeyB length: " + key.length * 8);
        System.out.println("Compressed KeyB: " + encoded.length + ":" + Arrays.toString(encoded));

        long[] keyBDec = new long[3];
        keyBDec[0] = prev[0] + toLong(encoded, 1 + maxNumBytes * 0, maxNumBytes);
        keyBDec[1] = prev[1] + toLong(encoded, 1 + maxNumBytes * 1, maxNumBytes);
        keyBDec[2] = prev[2] + toLong(encoded, 1 + maxNumBytes * 2, maxNumBytes);

        System.out.println(Arrays.toString(keyBDec));
    }

    public int numberOfBytes(long value){
        return (int) (Math.ceil(Math.log(value) / Math.log(2)) / 8) + 1;
    }

    public static void toBytes(long val, byte[] dest,int position, int numberOfBytes) { //rewrite this to put bytes in a already made array at the right position.
        for (int i = numberOfBytes - 1; i > 0; i--) {
            dest[position + i] = (byte) val;
            val >>>= 8;
        }
        dest[position] = (byte) val;
    }
    public static byte[] toBytes(long val) {
        int numberOfBytes = (int) (Math.ceil(Math.log(val) / Math.log(2)) / 8) + 1;
        byte [] b = new byte[numberOfBytes];
        for (int i = numberOfBytes - 1; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    public static long toLong(byte[] bytes) {
        long l = 0;
        for(int i = 0; i < bytes.length; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
           }
        return l;
    }

    public static long toLong(byte[] bytes, int offset, int length) {
        long l = 0;
        for(int i = offset; i < offset + length; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }
}
