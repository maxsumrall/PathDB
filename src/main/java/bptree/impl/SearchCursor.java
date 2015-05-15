package bptree.impl;

import bptree.PageProxyCursor;

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
    public long pageID;

    public SearchCursor(long pageID, long siblingNode, int position, LongBuffer keys, long[] searchKey, int keyLength){
        this.siblingNode = siblingNode;
        this.keys = keys;
        this.searchKey = searchKey;
        this.keyLength = keyLength;
        this.position = position * keyLength;
        this.capacity = keys.capacity();
        this.pageID = pageID;
    }

    public long[] next(PageProxyCursor cursor) throws IOException {
        long[] next = getNext(cursor);
        if(next != null){
            position += keyLength;
        }
        return next;

    }

    private long[] getNext(PageProxyCursor cursor) throws IOException {
        long[] currentKey = new long[keyLength];
        if(position < capacity){
            for(int i = 0; i < keyLength; i++){
                currentKey[i] = keys.get(position + i);
            }
        }
        else{
            if(siblingNode != -1) {
                loadSiblingNode(cursor);
                return getNext(cursor);
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

    public boolean hasNext(PageProxyCursor cursor) throws IOException {
        return getNext(cursor) != null;
    }


    private void loadSiblingNode(PageProxyCursor cursor) throws IOException {
        cursor.next(siblingNode);
        this.siblingNode = NodeHeader.getSiblingID(cursor);
        this.position = 0;
        //this.position = NodeSearch.search(cursor, searchKey)[0];//TODO this is likely not necessary, keys will always be at beginning.
        byte[] keysB = new byte[NodeHeader.getNumberOfKeys(cursor) * keyLength *  8];
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        cursor.getBytes(keysB);
        this.keys = ByteBuffer.wrap(keysB).asLongBuffer();
        this.capacity = keys.capacity();
    }
}
