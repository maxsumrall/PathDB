package bptree.impl;

import bptree.PageProxyCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;


public class NodeSearch {
    private static PrimitiveLongArray arrayUtil = new PrimitiveLongArray();
    public  PageProxyCursor cursor;
    public NodeTree tree;

    public NodeSearch(NodeTree tree){
        this.tree = tree;
    }
    public SearchCursor find(long[] key){
        long[] entry = null;
        SearchCursor resultsCursor = null;
        int[] searchResult;
        try (PageProxyCursor cursor = tree.disk.getCursor(tree.rootNodeId, PagedFile.PF_SHARED_LOCK)) {
                    searchResult = find(cursor, key);
                    long currentNode = cursor.getCurrentPageId();
                    if(searchResult[0] == 0) {
                        int[] altResult = moveCursorBackIfPreviousNodeContainsValidKeys(cursor, key);
                        if (currentNode != cursor.getCurrentPageId()) {
                            searchResult = altResult;
                        }
                    }
                    resultsCursor = new SearchCursor(cursor.getCurrentPageId(), NodeHeader.getSiblingID(cursor), searchResult[0], key, NodeHeader.getKeyLength(cursor), NodeHeader.getNumberOfKeys(cursor));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultsCursor;
    }

    public SearchCursor findWithCursor(PageProxyCursor cursor, long[] key){
        SearchCursor resultsCursor = null;
        int[] searchResult;
        try {
            cursor.next(tree.rootNodeId);
            searchResult = find(cursor, key);
            long currentNode = cursor.getCurrentPageId();
            if(searchResult[0] == 0) {
                int[] altResult = moveCursorBackIfPreviousNodeContainsValidKeys(cursor, key);
                if (currentNode != cursor.getCurrentPageId()) {
                    searchResult = altResult;
                }
            }
            resultsCursor = new SearchCursor(cursor.getCurrentPageId(), NodeHeader.getSiblingID(cursor), searchResult[0], key, NodeHeader.getKeyLength(cursor), NodeHeader.getNumberOfKeys(cursor));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultsCursor;
    }

    public int[] find(PageProxyCursor cursor, long[] key) throws IOException {
        int[] searchResult;
        if(NodeHeader.isLeafNode(cursor)){
            searchResult = search(cursor, key);
        }
        else{
            int index = search(cursor, key)[0];
            long child = tree.getChildIdAtIndex(cursor, index);
            cursor.next(child);
            searchResult = find(cursor, key);
        }
        return searchResult;
    }

    public int[] search(long nodeId, long[] key) {
        int[] result = new int[]{-1, -1};
        try (PageProxyCursor cursor = tree.disk.getCursor(nodeId, PagedFile.PF_SHARED_LOCK)) {
                    result = search(cursor, key);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    public static int[] search(PageProxyCursor cursor, long[] key){
        if(NodeHeader.isLeafNode(cursor)){
            return searchLeafNodeSameLengthKeys(cursor, key);
        }
        else{
            return searchInternalNodeSameLengthKeys(cursor, key);
        }
    }

    private static int[] searchInternalNodeSameLengthKeys(PageProxyCursor cursor, long[] key){
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


    private static int[] searchLeafNodeSameLengthKeys(PageProxyCursor cursor, long[] key){
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


    private static int[] moveCursorBackIfPreviousNodeContainsValidKeys(PageProxyCursor cursor, long[] key) throws IOException {
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
