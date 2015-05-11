package bptree.impl;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * Created by max on 5/6/15.
 */
public class SearchCursor {
    long siblingNode;
    int position;
    LongBuffer keys;
    int capacity;
    int keyLength;
    long[] searchKey;
    NodeTree proxy;

    public SearchCursor(long siblingNode, int position, LongBuffer keys, long[] searchKey, int keyLength){
        this.siblingNode = siblingNode;
        this.keys = keys;
        this.searchKey = searchKey;
        this.keyLength = keyLength;
        this.position = position * keyLength;
        this.capacity = keys.capacity();
    }

    public long[] next(){
        long[] next = getNext();
        if(next != null){
            position += keyLength;
        }
        return next;

    }

    private long[] getNext(){
        long[] currentKey = new long[keyLength];
        if(position < capacity){
            for(int i = 0; i < keyLength; i++){
                currentKey[i] = keys.get(position + i);
            }
        }
        else{
            if(siblingNode != -1) {
                loadSiblingNode();
                return getNext();
            }
            else{
                return null;
            }
        }
        if(Node.keyComparator.validPrefix(searchKey, currentKey)){
            return currentKey;
        }
        return null;
    }

    public boolean hasNext(){
        return getNext() != null;
    }


    private void loadSiblingNode(){
        try (PageCursor cursor = DiskCache.pagedFile.io(siblingNode, PagedFile.PF_SHARED_LOCK)) {
            if (cursor.next()) {
                do {
                    this.siblingNode = NodeHeader.getSiblingID(cursor);
                    this.position = NodeSearch.search(cursor, searchKey)[0];
                    byte[] keysB = new byte[NodeHeader.getNumberOfKeys(cursor) * keyLength *  8];
                    cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                    cursor.getBytes(keysB);
                    this.keys = ByteBuffer.wrap(keysB).asLongBuffer();
                    this.capacity = keys.capacity();
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
