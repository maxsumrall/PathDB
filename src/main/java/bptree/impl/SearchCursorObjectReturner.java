package bptree.impl;

import bptree.PageProxyCursor;

import java.io.IOException;

/**
 * Created by max on 5/6/15.
 */
public class SearchCursorObjectReturner {
    long siblingNode;
    int currentKeyIndex;
    int keyLength;
    Long[] searchKey;
    public long pageID;
    int keysInNode;

    public SearchCursorObjectReturner(SearchCursor properSearchCursor){
        this.siblingNode = properSearchCursor.siblingNode;
        this.searchKey = longArrBoxing(properSearchCursor.searchKey);
        this.keyLength = properSearchCursor.keyLength;
        this.currentKeyIndex = properSearchCursor.currentKeyIndex;
        this.pageID = properSearchCursor.pageID;
        this.keysInNode = properSearchCursor.keysInNode;
    }

    public Long[] next(PageProxyCursor cursor) throws IOException {
        Long[] next = getNext(cursor);
        if(next != null){
            currentKeyIndex++;
        }
        return next;
    }

    private Long[] getNext(PageProxyCursor cursor) throws IOException {
        Long[] currentKey = new Long[keyLength];
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

    private Long[] longArrBoxing(long[] arr){
        Long[] obj = new Long[arr.length];
        for(int i = 0; i < arr.length; i++)
            obj[i] = arr[i];
        return obj;
    }
}
