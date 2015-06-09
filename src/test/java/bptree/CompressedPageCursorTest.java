package bptree;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by max on 6/8/15.
 */
public class CompressedPageCursorTest {

    @Test
    public void multipleTests(){
        int keyLength = 3;
        long[][] keys = new long[][]{
                {1,2,3},
                {1,2,3},
                {5,4,6},
                {6,5,7},
                {7,6,8},
                {8,7,9}
        };

        int uncompressedLength = keys.length * 3 * 8;

        byte[] compressed = new byte[uncompressedLength];
        ByteBuffer buffer = ByteBuffer.wrap(compressed);

        buffer.put(encodeKey(keys[0], new long[]{0,0,0}));
        for(int i = 1; i < keys.length; i++)
            buffer.put(encodeKey(keys[i], keys[i-1]));

        int compressedLength = buffer.position();
        System.out.println("Uncompressed:" + uncompressedLength + " Compressed: " + compressedLength);

        long[][] dKeys = new long[keys.length][3];

        int position = 0;
        int reqBytes = compressed[position];
        position += 1 + (reqBytes * keyLength);
        for(int i = 0; i < keyLength; i++) {
            dKeys[0][i] = toLong(compressed, i + 1, reqBytes);
        }
        for(int i = 1; i < keys.length; i++){
            reqBytes = compressed[position++];
            for(int j = 0; j < keyLength; j++){
                dKeys[i][j] = dKeys[i-1][j] + toLong(compressed, position, reqBytes);
                position += reqBytes;
            }
        }

        for(int i = 0; i < keys.length; i++)
            System.out.print(Arrays.toString(keys[i]) + " -> " + Arrays.toString(dKeys[i]) + "\n");


    }

    @Ignore
    public void bitTesting() {

        int keyLength = 3;

        long[] keyA = new long[]{123,321,451};
        long[] keyB = new long[]{124,322,452};
        System.out.println(Arrays.toString(keyB));

        encodeKey(keyB, keyA);

    }

    public byte[] compress(long[][] keys){
        int maxCompressedSize = keys.length * keys[0].length * Long.BYTES;
        byte[] compressed = new byte[maxCompressedSize];
        ByteBuffer buffer = ByteBuffer.wrap(compressed);
        buffer.put(encodeKey(keys[0], new long[keys[0].length]));
        for(int i = 1; i < keys.length; i++)
            buffer.put(encodeKey(keys[i], keys[i-1]));
        byte[] truncatedCompressed = new byte[buffer.position()];
        System.arraycopy(compressed, 0, truncatedCompressed, 0, truncatedCompressed.length);
        return truncatedCompressed;
    }


    public byte[] encodeKey(long[] key, long[] prev){

        long[] diff = new long[key.length];
        for(int i = 0; i < key.length; i++)
        {
            diff[i] = key[i] - prev[i];
        }

        int maxNumBytes = Math.max(numberOfBytes(diff[0]), 1);
        for(int i = 1; i < key.length; i++){
            maxNumBytes = Math.max(maxNumBytes, numberOfBytes(diff[i]));
        }

        byte[] encoded = new byte[1 + (maxNumBytes * 3 )];
        encoded[0] = (byte)maxNumBytes;
        for(int i = 0; i < key.length; i++){
            toBytes(diff[i], encoded, 1 + (i * maxNumBytes), maxNumBytes);
        }

        //System.out.println("Original KeyB length: " + key.length * 8);
        //System.out.println("Compressed KeyB: " + encoded.length + ":" + Arrays.toString(encoded));

        /*long[] keyBDec = new long[3];
        keyBDec[0] = prev[0] + toLong(encoded, 1 + maxNumBytes * 0, maxNumBytes);
        keyBDec[1] = prev[1] + toLong(encoded, 1 + maxNumBytes * 1, maxNumBytes);
        keyBDec[2] = prev[2] + toLong(encoded, 1 + maxNumBytes * 2, maxNumBytes);
*/
        return encoded;
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
