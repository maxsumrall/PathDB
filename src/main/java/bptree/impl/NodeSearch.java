package bptree.impl;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * Created by max on 5/8/15.
 */
public class NodeSearch {
    private static PrimitiveLongArray arrayUtil = new PrimitiveLongArray();

    public static ProxyCursor find(long[] key){
        long[] entry = null;
        ProxyCursor resultsCursor = null;
        int[] searchResult;
        try (PageCursor cursor = NodeTree.pagedFile.io(NodeTree.rootNodeId, PagedFile.PF_SHARED_LOCK)) {
            if (cursor.next()) {
                do {
                    searchResult = find(cursor, key);
                    long currentNode = cursor.getCurrentPageId();
                    if(searchResult[0] == 0) {
                        int[] altResult = moveCursorBackIfPreviousNodeContainsValidKeys(cursor, key);
                        if (currentNode != cursor.getCurrentPageId()) {
                            searchResult = altResult;
                        }
                    }
                    byte[] keys = new byte[NodeHeader.getNumberOfKeys(cursor) * NodeHeader.getKeyLength(cursor) * 8];
                    cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                    cursor.getBytes(keys);
                    LongBuffer keysLB = ByteBuffer.wrap(keys).asLongBuffer();
                    resultsCursor = new ProxyCursor(NodeHeader.getSiblingID(cursor), searchResult[0], keysLB, key, NodeHeader.getKeyLength(cursor));
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultsCursor;
    }

    public static int[] find(PageCursor cursor, long[] key) throws IOException {
        int[] searchResult;
        if(NodeHeader.isLeafNode(cursor)){
            searchResult = search(cursor, key);
        }
        else{
            int index = search(cursor, key)[0];
            long child = NodeTree.getChildIdAtIndex(cursor, index);
            cursor.next(child);
            searchResult = find(cursor, key);
        }
        return searchResult;
    }

    public static int[] search(long nodeId, long[] key) {
        int[] result = new int[]{-1, -1};
        try (PageCursor cursor = NodeTree.pagedFile.io(nodeId, PagedFile.PF_SHARED_LOCK)) {
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


    public static int[] search(PageCursor cursor, long[] key){
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

    private static int[] searchInternalNodeSameLengthKeys(PageCursor cursor, long[] key){
        int index = -1;
        int offset = -1;
        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        if(numberOfKeys == 0){
            return new int[]{0, NodeHeader.NODE_HEADER_LENGTH};
        }
        int keyLength = NodeHeader.getKeyLength(cursor);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + ((numberOfKeys + 1) * 8)); //header + children
        long[] currKey = new long[keyLength];
        for(int i = 0; i < numberOfKeys; i++){
            for(int j = 0; j < keyLength; j++) {
                currKey[j] = cursor.getLong();
            }
            if(NodeTree.comparator.prefixCompare(key, currKey) < 0){
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
    private static int[] searchInternalNodeDifferentLengthKeys(PageCursor cursor, long[] key){
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
            if(NodeTree.comparator.prefixCompare(key, arrayUtil.get()) < 0){
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

    private static int[] searchLeafNodeSameLengthKeys(PageCursor cursor, long[] key){
        int index = -1;
        int offset = -1;
        int numberOfKeys = cursor.getInt(NodeHeader.BYTE_POSITION_KEY_COUNT);
        int keyLength = NodeHeader.getKeyLength(cursor);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        long[] currKey = new long[keyLength];
        for(int i = 0; i < numberOfKeys; i++){
            for(int j = 0; j < keyLength; j++) {
                currKey[j] = cursor.getLong();
            }
            if(NodeTree.comparator.prefixCompare(key, currKey) <= 0){
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

    private static int[] searchLeafNodeDifferentLengthKeys(PageCursor cursor, long[] key){
        int index = -1;
        int offset = -1;
        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
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
            if(NodeTree.comparator.prefixCompare(key, arrayUtil.get()) == 0){
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

    private static int[] moveCursorBackIfPreviousNodeContainsValidKeys(PageCursor cursor, long[] key) throws IOException {
        long currentNode = cursor.getCurrentPageId();
        long previousNode = NodeHeader.getPrecedingID(cursor);
        if(previousNode != -1){
            cursor.next(previousNode);
        }
        int[] result = search(cursor, key);
        if(result[0] == NodeHeader.getNumberOfKeys(cursor)){
            cursor.next(currentNode);
        }
        return result;
    }
}
