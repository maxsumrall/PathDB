package bptree.impl;

import bptree.PageProxyCursor;

import java.io.IOException;

/**
 * Created by max on 5/6/15.
 */
public class SearchCursor {
    long siblingNode;
    int currentKeyIndex;
    int keyLength;
    long[] searchKey;
    public long pageID;
    int keysInNode;


    public SearchCursor(long pageID, long siblingNode, int position, long[] searchKey, int keyLength, int keysInNode){
        this.siblingNode = siblingNode;
        this.searchKey = searchKey;
        this.keyLength = keyLength;
        this.currentKeyIndex = position;
        this.pageID = pageID;
        this.keysInNode = keysInNode;
    }

    public long[] next(PageProxyCursor cursor) throws IOException {
        long[] next = getNext(cursor);
        if(next != null){
            currentKeyIndex++;
        }
        return next;
    }

    private long[] getNext(PageProxyCursor cursor) throws IOException {
        long[] currentKey = new long[keyLength];
        if(currentKeyIndex < keysInNode){
            for(int i = 0; i < keyLength; i++){
                int bytePosition = NodeHeader.NODE_HEADER_LENGTH + (currentKeyIndex * keyLength * 8) + (i * 8);
                currentKey[i] = cursor.getLong(bytePosition);
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
        this.pageID = siblingNode;
        this.keysInNode = NodeHeader.getNumberOfKeys(cursor);
        this.currentKeyIndex = 0;
        this.siblingNode = NodeHeader.getSiblingID(cursor);
    }
}
