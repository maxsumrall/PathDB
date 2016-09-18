/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package PathIndex;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class CompressionExample {
    int maxNumBytes;
    final int sameID = 128;
    final int sameFirstNode = 64;
    @Test
    public void multipleTests(){
        int keyLength = 4;
        long[][] keys = new long[][]{
                {1,0,3,4},
                {57,36983,0,558259},
                {57,36984,0,558034},
                {57,36985,1000000,558034},
                {57,36986,0,658033}
        };

        int uncompressedLength = keys.length * 4 * 8;

        byte[] compressed = new byte[uncompressedLength];
        ByteBuffer buffer = ByteBuffer.wrap(compressed);

        buffer.put(encodeKey(keys[0], new long[]{0,0,0,0}));
        for(int i = 1; i < keys.length; i++)
            buffer.put(encodeKey(keys[i], keys[i-1]));

        int compressedLength = buffer.position();
        System.out.println("Uncompressed:" + uncompressedLength + " Compressed: " + compressedLength);

        long[][] dKeys = new long[keys.length][4];

        int position = 0;

        byte header = compressed[position];
        int firstEncodedIndex = 0;
        boolean samePath = (sameID & header) == sameID;
        boolean sameFirstID = (sameFirstNode & header) == sameFirstNode;
        if(samePath)
            firstEncodedIndex++;
        if(sameFirstID)
            firstEncodedIndex++;
        header &= ~(1 << 7);
        header &= ~(1 << 6);
        int reqBytes = header;

        position += 1 + (reqBytes * (keyLength - firstEncodedIndex));
        for(int i = 0; i < keyLength; i++) {
            dKeys[0][i] = toLong(compressed, i + 1, reqBytes);
        }
        for(int i = 1; i < keys.length; i++){
            header = compressed[position++];

            //
            firstEncodedIndex = 0;
            samePath = (sameID & header) == sameID;
            sameFirstID = (sameFirstNode & header) == sameFirstNode;
            if(samePath) {
                firstEncodedIndex++;
                dKeys[i][0] = dKeys[i-1][0];
            }
            if(sameFirstID) {
                firstEncodedIndex++;
                dKeys[i][1] = dKeys[i-1][1];
            }
            header &= ~(1 << 7);
            header &= ~(1 << 6);
            reqBytes = header;
            //

            for(int j = firstEncodedIndex; j < (keyLength); j++){
                dKeys[i][j] = dKeys[i-1][j] + toLong(compressed, position, reqBytes);
                position += reqBytes;
            }
        }

        for(int i = 0; i < keys.length; i++)
            System.out.print(Arrays.toString(keys[i]) + " -> " + Arrays.toString(dKeys[i]) + "\n");


    }
    public byte[] encodeKey(long[] key, long[] prev){
        this.maxNumBytes = 0;
        int firstEncodedIndex = 0;
        byte header = (byte)0;
        if(key[0] == prev[0]) {
            //set first bit
            firstEncodedIndex++;
            header |= sameID;

            if (key[1] == prev[1]) {
                //set second bit
                firstEncodedIndex++;
                header |= sameFirstNode;
            }
        }

        for(int i = firstEncodedIndex; i < key.length; i++){
            maxNumBytes = Math.max(maxNumBytes, numberOfBytes(key[i] - prev[i]));
        }

        byte[] encoded = new byte[1 + (maxNumBytes * (key.length - firstEncodedIndex))];
        header |=  maxNumBytes;
        encoded[0] = header;
        for(int i = 0; i < key.length - firstEncodedIndex; i++){
            toBytes(key[i + firstEncodedIndex] - prev[i + firstEncodedIndex], encoded, 1 + (i * maxNumBytes), maxNumBytes);
        }
        return encoded;
    }

    public int numberOfBytes(long value){
        long abs = Math.abs(value);
        int minBytes = 8;
        if(abs <= 127){
            minBytes = 1;
        }
        else if(abs <= 32768){
            minBytes = 2;
        }
        else if(abs <= 8388608){
            minBytes = 3;
        }
        else if(abs <= 2147483648l){
            minBytes = 4;
        }
        else if(abs <= 549755813888l){
            minBytes = 5;
        }
        else if(abs <= 140737488355328l){
            minBytes = 6;
        }
        else if(abs <= 36028797018963968l){
            minBytes = 7;
        }
        //return minBytes;
        return (int) Math.ceil(Math.ceil(log2nlz(value) / 8.0));
    }
    public static int log2nlz( long bits )
    {
        if( bits == 0 )
            return 0; // or throw exception
        return 31 - Long.numberOfLeadingZeros( bits );
    }

    public static void toBytes(long val, byte[] dest,int position, int numberOfBytes) { //rewrite this to put bytes in a already made array at the right position.
        for (int i = numberOfBytes - 1; i > 0; i--) {
            dest[position + i] = (byte) val;
            val >>= 8;
        }
        dest[position] = (byte) val;
    }

    public static long toLong(byte[] bytes, int offset, int length) {
        long l = bytes[offset] < (byte)0 ? -1 : 0;
        for (int i = offset; i < offset + length; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }
}
