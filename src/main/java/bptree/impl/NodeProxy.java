package bptree.impl;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.util.Arrays;

/**
 * Static class for manipulating nodes without doing any object instantiation.
 */
public class NodeProxy {

    public PagedFile pagedFile;
    public static KeyImpl comparator = new KeyImpl();
    private PrimitiveLongArray arrayUtil = new PrimitiveLongArray();

    public void setPagedFile(PagedFile pagedFile){
        this.pagedFile = pagedFile;
    }

    public void setPrecedingId(long nodeId, long newPrecedingId){
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    NodeHeader.setPrecedingId(cursor, newPrecedingId);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setFollowingId(long nodeId, long newFollowingId){
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    NodeHeader.setFollowingID(cursor, newFollowingId);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getChildIdAtIndex(long nodeId, int indexOfChild){
        long childId = 0;
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_SHARED_LOCK)) {
            if (cursor.next()) {
                do {
                    childId = cursor.getLong(NodeHeader.NODE_HEADER_LENGTH + indexOfChild * 8);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return childId;
    }


    public void addKeyToLeafNode(long nodeId, long[] key){
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    int offset = search(cursor, key)[1];
                    updateHeader(cursor, key);
                    insertKeyAtIndex(cursor, offset, key);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void insertKeyAtIndex(PageCursor cursor, int offset, long[] key){
        byte[] tmp_bytes = new byte[DiskCache.PAGE_SIZE - offset - key.length * 8];
        cursor.setOffset(offset);
        cursor.getBytes(tmp_bytes);
        cursor.setOffset(offset);
        for(long item : key){
            cursor.putLong(item);
        }
        cursor.putBytes(tmp_bytes);

    }

    private void updateHeader(PageCursor cursor, long[] key){
        if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
            if(NodeHeader.getNumberOfKeys(cursor) > 0 && NodeHeader.getKeyLength(cursor) != key.length){
                NodeHeader.setKeyLength(cursor, -1);
            }
            else if(NodeHeader.getNumberOfKeys(cursor) == 0){
                NodeHeader.setKeyLength(cursor, key.length);
            }
        }
        NodeHeader.setNumberOfKeys(cursor, NodeHeader.getNumberOfKeys(cursor) + 1);
    }

    public int[] search(long nodeId, long[] key) {
        int[] result = new int[]{-1, -1};
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_SHARED_LOCK)) {
            if (cursor.next()) {
                do {
                    result = search(cursor, key);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    public int[] search(PageCursor cursor, long[] key){
        if(NodeHeader.isLeafNode(cursor)){
            if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
                return searchLeafNodeSameLengthKeys(cursor, key);
            }
            else{
                return searchLeafNodeDifferentLengthKeys(cursor, key);
            }

        }
        else{
            if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
                return searchInternalNodeSameLengthKeys(cursor, key);
            }
            else{
                return searchInternalNodeDifferentLengthKeys(cursor, key);
            }
        }
    }

    private int[] searchInternalNodeSameLengthKeys(PageCursor cursor, long[] key){
        int index = -1;
        int offset = -1;
        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        int keyLength = NodeHeader.getKeyLength(cursor);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + ((numberOfKeys + 1) * 8)); //header + children
        long[] currKey = new long[keyLength];
        for(int i = 0; i < numberOfKeys; i++){
            Arrays.fill(currKey, 0l);
            for(int j = 0; j < keyLength; j++) {
                    currKey[j] = cursor.getLong();
                }
            if(comparator.prefixCompare(key, currKey) < 0){
                index = i;
                offset = cursor.getOffset() - (8 * keyLength);
                break;
            }
        }
        if(index == -1){ //Didn't find anything
            index = numberOfKeys;
            offset = cursor.getOffset();
        }
        return new int[]{index, offset};
    }
    private int[] searchInternalNodeDifferentLengthKeys(PageCursor cursor, long[] key){
        int index = -1;
        int offset = -1;
        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        int keyLength = NodeHeader.getKeyLength(cursor);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + ((numberOfKeys + 1) * 8)); //header + children
        long currKey;
        int lastKeyLength; //for rewinding of offset to return.
        for(int i = 0; i < numberOfKeys; i++) {
            lastKeyLength = 1;
            currKey = cursor.getLong();
            while(currKey != Node.KEY_DELIMITER) {
                arrayUtil.put(currKey);
                currKey = cursor.getLong();
                lastKeyLength++;
            }
            if(comparator.prefixCompare(key, arrayUtil.get()) < 0){
                index = i;
                offset = cursor.getOffset() - (8 * lastKeyLength);
                break;
            }
        }
        if(index == -1){ //Didn't find anything
            index = numberOfKeys;
            offset = cursor.getOffset();
        }
        return new int[]{index, offset};
    }

    private int[] searchLeafNodeSameLengthKeys(PageCursor cursor, long[] key){
        int index = -1;
        int offset = -1;
        int numberOfKeys = cursor.getInt(NodeHeader.BYTE_POSITION_KEY_COUNT);
        int keyLength = NodeHeader.getKeyLength(cursor);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        long[] currKey = new long[keyLength];
        for(int i = 0; i < numberOfKeys; i++){
            Arrays.fill(currKey, 0l);
            for(int j = 0; j < keyLength; j++) {
                currKey[j] = cursor.getLong();
            }
            if(comparator.prefixCompare(key, currKey) < 0){
                index = i;
                offset = cursor.getOffset() - (8 * keyLength);
                break;
            }
        }
        if(index == -1){ //Didn't find anything
            index = numberOfKeys;
            offset = cursor.getOffset();
        }
        return new int[]{index, offset};
    }

    private int[] searchLeafNodeDifferentLengthKeys(PageCursor cursor, long[] key){
        int index = -1;
        int offset = -1;
        int numberOfKeys = cursor.getInt(NodeHeader.BYTE_POSITION_KEY_COUNT);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        long currKey;
        int lastKeyLength; //for rewinding of offset to return.
        for(int i = 0; i < numberOfKeys; i++) {
            lastKeyLength = 1;
            currKey = cursor.getLong();
            while(currKey != Node.KEY_DELIMITER) {
                arrayUtil.put(currKey);
                currKey = cursor.getLong();
                lastKeyLength++;
            }
            if(comparator.prefixCompare(key, arrayUtil.get()) < 0){
                index = i;
                offset = cursor.getOffset() - (8 * lastKeyLength);
                break;
            }
        }
        if(index == -1){ //Didn't find anything
            index = numberOfKeys;
            offset = cursor.getOffset();
        }
        return new int[]{index, offset};
    }

    public boolean leafNodeContainsSpaceForNewKey(long nodeId, long[] newKey){
        return leafNodeByteSize(nodeId, newKey) < DiskCache.PAGE_SIZE;
    }

    public int leafNodeByteSize(long nodeId, long[] newKey){
        int byteSize = 0;
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
                    byteSize += NodeHeader.NODE_HEADER_LENGTH;
                    if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
                        int keyLength = NodeHeader.getKeyLength(cursor);
                        byteSize += ((numberOfKeys + 1) * keyLength * 8);
                    }
                    else{
                        long currKey = 0;
                        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                        for(int i = 0; i < numberOfKeys; i++) {
                            currKey = cursor.getLong();
                            byteSize+=8;
                            while(currKey != Node.KEY_DELIMITER) {
                                byteSize += 8;
                                currKey = cursor.getLong();
                            }
                        }
                        byteSize += (newKey.length + 1) * 8;
                    }
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteSize;
    }

    public boolean internalNodeContainsSpaceForNewKeyAndChild(long nodeId, long[] newKey){
        return internalNodeByteSize(nodeId, newKey) < DiskCache.PAGE_SIZE;
    }

    public int internalNodeByteSize(long nodeId, long[] newKey){
        int byteSize = 0;
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
                    byteSize += NodeHeader.NODE_HEADER_LENGTH;
                    byteSize += (numberOfKeys + 2) * 8; //calculate number of children;
                    if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
                        int keyLength = NodeHeader.getKeyLength(cursor);
                        byteSize += ((numberOfKeys + 1) * keyLength * 8);
                    }
                    else{
                        long currKey = 0;
                        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + (numberOfKeys + 1) * 8);
                        for(int i = 0; i < numberOfKeys; i++) {
                            currKey = cursor.getLong();
                            byteSize+=8;
                            while(currKey != Node.KEY_DELIMITER) {
                                byteSize += 8;
                                currKey = cursor.getLong();
                            }
                        }
                        byteSize += (newKey.length + 1) * 8;
                    }
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteSize;
    }

    private class PrimitiveLongArray {
        private long[] tmp_array = new long[20]; //temporary array much longer than ever necessary.
        private int index = 0;
        public void put(long item){
            tmp_array[index++] = item;
        }
        public long[] get(){
            long[] ret = new long[index];
            System.arraycopy(tmp_array, 0, ret, 0, index);
            index = 0;
            Arrays.fill(tmp_array, 0);
            return ret;
        }
    }

}
