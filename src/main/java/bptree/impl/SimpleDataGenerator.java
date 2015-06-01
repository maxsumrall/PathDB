package bptree.impl;

import bptree.BulkLoadDataSource;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;


public class SimpleDataGenerator implements BulkLoadDataSource {

    public int numberOfPages;
    public int currentPage = 0;
    public long currentKey = 0;
    int keyLength = 4;
    byte[] keysInPage = new byte[1000 * 4 * 8]; //just random guess for 1000
    ByteBuffer bb = ByteBuffer.wrap(keysInPage);
    LongBuffer lb = bb.asLongBuffer();

    public SimpleDataGenerator(int numberOfPages){
        this.numberOfPages = numberOfPages;
    }

    @Override
    public byte[] nextPage() {
        lb.position(0);
        long[] key = new long[keyLength];
        for (long j = 0; j < 1000; j++) {
            for (int k = 0; k < key.length; k++) {
                key[k] = currentKey;
            }
            lb.put(key);
            currentKey++;
        }
        currentPage++;
        return keysInPage;
    }


    @Override
    public boolean hasNext() {
        return currentPage <= numberOfPages;
    }
}