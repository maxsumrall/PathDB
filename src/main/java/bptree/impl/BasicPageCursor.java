package bptree.impl;

import bptree.PageProxyCursor;
import org.neo4j.io.pagecache.PageCursor;

import java.io.IOException;

/**
 * Created by max on 5/11/15.
 */
public class BasicPageCursor extends PageProxyCursor {

    PageCursor cursor;

    public BasicPageCursor(DiskCache disk, long pageId, int lock) throws IOException {
        this.cursor = disk.pagedFile.io(pageId, lock);
        cursor.next();
    }

    @Override
    public void next(long page) throws IOException {
        cursor.next(page);
    }

    @Override
    public long getCurrentPageId() {
        return cursor.getCurrentPageId();
    }

    @Override
    public int capacity() {
        return DiskCache.PAGE_SIZE;
    }

    @Override
    public int getSize(){
        return DiskCache.PAGE_SIZE;
    }

    @Override
    public void setOffset(int offset) {
        cursor.setOffset(offset);
    }

    @Override
    public int getOffset() {
        return cursor.getOffset();
    }

    @Override
    public void getBytes(byte[] dest) {
        cursor.getBytes(dest);
    }

    @Override
    public byte getByte(int offset) {
        return cursor.getByte(offset);
    }

    @Override
    public void putBytes(byte[] src) {
        cursor.putBytes(src);
    }

    @Override
    public void putByte(byte val){
        cursor.putByte(val);
    }

    @Override
    public void putByte(int offset, byte val){
        cursor.putByte(val);
    }

    @Override
    public long getLong() {
        return cursor.getLong();
    }

    @Override
    public long getLong(int offset) {
        return cursor.getLong(offset);
    }

    @Override
    public void putLong(long val){
        cursor.putLong(val);
    }
    @Override
    public void putLong(int offset, long val){
        cursor.putLong(offset, val);
    }

    @Override
    public int getInt() {
        return cursor.getInt();
    }

    @Override
    public int getInt(int offset) {
        return cursor.getInt(offset);
    }

    @Override
    public void putInt(int val){
        cursor.putInt(val);
    }
    @Override
    public void putInt(int offset, int val){
        cursor.putInt(offset, val);
    }
    @Override
    public boolean leafNodeContainsSpaceForNewKey(long[] newKey){
        return NodeSize.leafNodeContainsSpaceForNewKey(this, newKey);
    }

    @Override
    public void deferWriting() {
    }

    @Override
    public void resumeWriting() {
    }

    @Override
    public boolean internalNodeContainsSpaceForNewKeyAndChild(long[] newKey){
        return NodeSize.internalNodeContainsSpaceForNewKeyAndChild(this, newKey);
    }

    @Override
    public void close() throws IOException {
        this.cursor.close();
    }
}
