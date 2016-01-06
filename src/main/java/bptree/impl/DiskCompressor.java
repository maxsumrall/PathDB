/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package bptree.impl;

import PageCacheSort.SetIterator;
import bptree.PageProxyCursor;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;

public class DiskCompressor {
    public static int maxNumBytes;
    public static long finalPageID = 0;
    final static int sameID = 128;
    final static int sameFirstNode = 64;


    public static DiskCache convertDiskToCompressed(SetIterator iterator, int keyLength) throws IOException {
        long[] next;
        long[] prev = new long[keyLength];
        byte[] encodedKey;
        int keyCount = 0;
        DiskCache compressedDisk = DiskCache.persistentDiskCache( keyLength + "compressed_disk.dat", false); //I'm handling compression here, so I don't want the cursor to get confused.
        try(PageProxyCursor compressedCursor = compressedDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK)) {
            compressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            while(iterator.hasNext()){
                next = iterator.getNext();
                encodedKey = encodeKey(next, prev);
                System.arraycopy(next, 0, prev, 0, prev.length);
                keyCount++;
                compressedCursor.putBytes(encodedKey);
                if(compressedCursor.getOffset() + (keyLength * Long.BYTES) > DiskCache.PAGE_SIZE){
                    //finalize this buffer, write it to the page, and start a new buffer
                    NodeHeader.setFollowingID(compressedCursor, compressedCursor.getCurrentPageId() + 1);
                    NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
                    NodeHeader.setKeyLength(compressedCursor, keyLength);
                    NodeHeader.setNumberOfKeys(compressedCursor, keyCount);
                    NodeHeader.setNodeTypeLeaf(compressedCursor);
                    compressedCursor.next(compressedCursor.getCurrentPageId() + 1);
                    compressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                    keyCount = 0;
                    prev = new long[keyLength];
                }
            }
            NodeHeader.setFollowingID(compressedCursor, -1);//last leaf node
            NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
            NodeHeader.setKeyLength(compressedCursor, keyLength);
            NodeHeader.setNumberOfKeys(compressedCursor, keyCount);
            NodeHeader.setNodeTypeLeaf(compressedCursor);
            finalPageID = compressedCursor.getCurrentPageId();
        }
        compressedDisk.COMPRESSION = true;
        return compressedDisk;
    }

    public static byte[] encodeKey(long[] key, long[] prev){

        maxNumBytes = 0;
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


    public static int numberOfBytes(long value){
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
        return minBytes;
    }
    public static void toBytes(long val, byte[] dest, int position, int numberOfBytes) { //rewrite this to put bytes in a already made array at the right position.
        for (int i = numberOfBytes - 1; i > 0; i--) {
            dest[position + i] = (byte) val;
            val >>= 8;
        }
        dest[position] = (byte) val;
    }
}
