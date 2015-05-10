package bptree.impl;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;

/**
 * Created by max on 5/8/15.
 */
public class NodeSize {

    public static boolean leafNodeContainsSpaceForNewKey(long nodeId, long[] newKey){
        return leafNodeByteSize(nodeId, newKey) < DiskCache.PAGE_SIZE;
    }

    public static boolean leafNodeContainsSpaceForNewKey(PageCursor cursor, long[] newKey){
        return leafNodeByteSize(cursor, newKey) < DiskCache.PAGE_SIZE;
    }

    public static int leafNodeByteSize(long nodeId, long[] newKey){
        int size = 0;
        try (PageCursor cursor = NodeTree.pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    size = leafNodeByteSize(cursor, newKey);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return size;
    }

    public static int leafNodeByteSize(PageCursor cursor, long[] newKey){
        int byteSize = 0;
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
        return byteSize;
    }

    public boolean internalNodeContainsSpaceForNewKeyAndChild(long nodeId, long[] newKey){
        return internalNodeByteSize(nodeId, newKey) < DiskCache.PAGE_SIZE;
    }

    public static boolean internalNodeContainsSpaceForNewKeyAndChild(PageCursor cursor, long[] newKey){
        return internalNodeByteSize(cursor, newKey) < DiskCache.PAGE_SIZE;
    }

    public static int internalNodeByteSize(long nodeId, long[] newKey){
        int size = 0;
        try (PageCursor cursor = NodeTree.pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    size = internalNodeByteSize(cursor, newKey);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return size;
    }

    public static int internalNodeByteSize(PageCursor cursor, long[] newKey){
        int byteSize = 0;
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
        return byteSize;
    }
}
