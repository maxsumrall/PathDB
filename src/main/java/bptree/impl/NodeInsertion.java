package bptree.impl;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * Created by max on 5/8/15.
 */
public class NodeInsertion {

    private static PrimitiveLongArray arrayUtil = new PrimitiveLongArray();

    public static SplitResult insert(long[] key){
        SplitResult result = null;
        try (PageCursor cursor = NodeTree.pagedFile.io(NodeTree.rootNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    if(NodeHeader.isLeafNode(cursor)){
                        result = addKeyToLeafNode(cursor, cursor.getCurrentPageId(), key);
                    } else{
                        int index = NodeSearch.search(cursor, key)[0];
                        long child = NodeTree.getChildIdAtIndex(cursor, index);
                        long id = cursor.getCurrentPageId();
                        cursor.next(child);
                        result = insert(cursor, key);
                        if(result != null){
                            cursor.next(id);
                            result = addKeyAndChildToInternalNode(cursor, id, result.primkey, result.right);
                        }
                    }
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
    private static SplitResult insert(PageCursor cursor, long[] key) throws IOException {
        SplitResult result = null;
        if(NodeHeader.isLeafNode(cursor)){
            result = addKeyToLeafNode(cursor, cursor.getCurrentPageId(), key);
        }
        else{
            int index = NodeSearch.search(cursor, key)[0];
            long child = NodeTree.getChildIdAtIndex(cursor, index);
            long id = cursor.getCurrentPageId();
            cursor.next(child);
            result = insert(cursor, key);
            if(result != null){
                cursor.next(id);
                result = addKeyAndChildToInternalNode(cursor, id, result.primkey, result.right);
            }
        }
        return result;
    }
    public static SplitResult addKeyAndChildToInternalNode(long nodeId, long[] key, long child){
        SplitResult result = null;
        try (PageCursor cursor = NodeTree.pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    result = addKeyAndChildToInternalNode(cursor, nodeId, key, child);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static SplitResult addKeyAndChildToInternalNode(PageCursor cursor, long nodeId, long[] key, long child) throws IOException {
        SplitResult result = null;
        if(!NodeSize.internalNodeContainsSpaceForNewKeyAndChild(cursor, key)){
            long newInternalNodeId = NodeTree.acquireNewInternalNode(cursor);
            result = new SplitResult();
            result.left = nodeId;
            result.right = newInternalNodeId;
            NodeTree.updateSiblingAndFollowingIdsInsertion(cursor, nodeId, newInternalNodeId);
            result.primkey = insertAndBalanceKeysBetweenInternalNodes(cursor, nodeId, newInternalNodeId, key, child);
            if(!newKeyBelongsInNewNode(cursor, key)){
                cursor.next(nodeId);
            }
        }
        else{
            checkIfNodeRequiresDifferentLengthConversion(cursor, key);
            int[] searchResult = NodeSearch.search(cursor, key);
            insertKeyAtIndex(cursor, searchResult[1], key);
            insertChildAtIndex(cursor, searchResult[0] + 1, child);
            updateHeader(cursor, key);
        }
        return result;
    }

    public static SplitResult addKeyToLeafNode(long nodeId, long[] key){
        SplitResult result = null;
        try (PageCursor cursor = NodeTree.pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    result = addKeyToLeafNode(cursor, nodeId, key);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static SplitResult addKeyToLeafNode(PageCursor cursor, long nodeId, long[] key) throws IOException {
        SplitResult result = null;
        if(!NodeSize.leafNodeContainsSpaceForNewKey(cursor, key)){
            long newLeafNodeId = NodeTree.acquireNewLeafNode(cursor);
            result = new SplitResult();
            result.left = nodeId;
            result.right = newLeafNodeId;
            NodeTree.updateSiblingAndFollowingIdsInsertion(cursor, nodeId, newLeafNodeId);
            result.primkey = insertAndBalanceKeysBetweenLeafNodes(cursor, nodeId, newLeafNodeId, key);
        }
        else{
            checkIfNodeRequiresDifferentLengthConversion(cursor, key);
            int[] searchResult = NodeSearch.search(cursor, key);
            insertKeyAtIndex(cursor, searchResult[1], key);
            updateHeader(cursor, key);
        }
        return result;
    }
    private static long[] insertAndBalanceKeysBetweenLeafNodes(PageCursor cursor, long fullNode, long emptyNode, long[] newKey) throws IOException {
        //grab half of the keys from the first node, dump into the new node.
        cursor.next(fullNode);
        long[] returnedKey = null;
        byte[] keysA = null;
        byte[] keysB = null;

        int[] searchResults = NodeSearch.search(cursor, newKey);
        int keyLength = NodeHeader.getKeyLength(cursor);
        int originalNumberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        int keysInclInsert = originalNumberOfKeys + 1;
        if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
            returnedKey = new long[keyLength];
            byte[] keys = new byte[originalNumberOfKeys * keyLength * 8];
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            cursor.getBytes(keys);
            keys = insertKeyAtIndex(keys, newKey, searchResults[0], returnedKey);
            keysA = new byte[((keysInclInsert/2) * 4) * 8];
            keysB = new byte[(((keysInclInsert + 1) /2 ) * 4) * 8];
            int middle = (keys.length / 2);
            System.arraycopy(keys, 0, keysA, 0, keysA.length);
            System.arraycopy(keys, middle, keysB, 0, keysB.length);

        }
        else{
            //Do it for delimited node

        }
        NodeHeader.setNumberOfKeys(cursor, keysInclInsert / 2);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        cursor.putBytes(keysA);

        cursor.next(emptyNode);
        NodeHeader.setNumberOfKeys(cursor, (keysInclInsert + 1) / 2);
        NodeHeader.setKeyLength(cursor, keyLength);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        cursor.putBytes(keysB);

        return returnedKey;
    }

    private static long[] insertAndBalanceKeysBetweenInternalNodes(PageCursor cursor, long fullNode, long emptyNode, long[] newKey, long newChild) throws IOException {
        //grab half of the keys from the first node, dump into the new node.
        cursor.next(fullNode);
        long[] returnedKey = null;
        byte[] childrenA = null;
        byte[] childrenB = null;
        byte[] keysA = null;
        byte[] keysB = null;

        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);

        int[] searchResults = NodeSearch.search(cursor, newKey);
        int keyLength = NodeHeader.getKeyLength(cursor);
        int originalNumberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        int keysInclInsert = originalNumberOfKeys + 1;
        if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
            returnedKey = new long[keyLength];
            byte[] keys = new byte[originalNumberOfKeys * keyLength * 8];
            byte[] children = new byte[(originalNumberOfKeys + 1) * 8];
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            cursor.getBytes(children);
            cursor.getBytes(keys);
            keys = insertKeyAtIndex(keys, newKey, searchResults[0], returnedKey);
            children = insertChildAtIndex(children, newChild, searchResults[0] + 1);
            int splitIndex = (originalNumberOfKeys + 1) / 2;
            childrenA = new byte[((originalNumberOfKeys + 3) / 2) * 8]; // 1 for normal, another 1 for the new key/child, and another one for rounding up in integer division.
            childrenB = new byte[((originalNumberOfKeys + 2) / 2) * 8];
            keysA = new byte[((keysInclInsert/2) * 4) * 8];
            keysB = new byte[((originalNumberOfKeys /2 ) * 4) * 8];
            int numberOfBytesInKey = keyLength * 8;
            int middleAfterMiddleKey = (keys.length / 2) + numberOfBytesInKey;
            System.arraycopy(keys, 0, keysA, 0, keysA.length);
            System.arraycopy(keys, middleAfterMiddleKey, keysB, 0, keysB.length);
            System.arraycopy(children, 0, childrenA, 0, childrenA.length);
            System.arraycopy(children, childrenA.length , childrenB, 0, childrenB.length);

        }
        else{
            //Do it for delimited node

        }
        NodeHeader.setNumberOfKeys(cursor, keysInclInsert / 2);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        cursor.putBytes(childrenA);
        cursor.putBytes(keysA);

        cursor.next(emptyNode);
        NodeHeader.setNumberOfKeys(cursor, originalNumberOfKeys / 2);
        NodeHeader.setKeyLength(cursor, keyLength);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        cursor.putBytes(childrenB);
        cursor.putBytes(keysB);

        return returnedKey;
    }

    private static boolean newKeyBelongsInNewNode(PageCursor cursor, long[] newKey){
        return NodeTree.comparator.prefixCompare(newKey, getFirstKeyInNode(cursor)) > 0;

    }
    public static long[] getFirstKeyInNode(PageCursor cursor){
        long[] firstKey;
        if(NodeHeader.isLeafNode(cursor)){
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        }
        else{
            int children = NodeHeader.getNumberOfKeys(cursor) + 1;
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + children * 8);
        }

        if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
            int keyLength = NodeHeader.getKeyLength(cursor);
            firstKey = new long[keyLength];
            for(int i = 0; i < keyLength; i++){
                firstKey[i] = cursor.getLong();
            }
        }
        else{
            long currKey = cursor.getLong();
            while(currKey != Node.KEY_DELIMITER) {
                arrayUtil.put(currKey);
                currKey = cursor.getLong();
            }
            firstKey = arrayUtil.get();
        }
        return firstKey;
    }
    public static byte[] getFirstKeyInNodeAsBytes(PageCursor cursor){
        byte[] firstKey;
        if(NodeHeader.isLeafNode(cursor)){
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        }
        else{
            int children = NodeHeader.getNumberOfKeys(cursor) + 1;
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + children * 8);
        }

        if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
            int keyLength = NodeHeader.getKeyLength(cursor);
            firstKey = new byte[keyLength * 8];
            cursor.getBytes(firstKey);
        }
        else{
            long currKey = cursor.getLong();
            while(currKey != Node.KEY_DELIMITER) {
                arrayUtil.put(currKey);
                currKey = cursor.getLong();
            }
            firstKey = arrayUtil.getAsBytes();
        }
        return firstKey;
    }
    private static void insertKeyAtIndex(PageCursor cursor, int offset, long[] key){
        byte[] tmp_bytes;
        if(NodeHeader.isNodeWithSameLengthKeys(cursor)) {
            tmp_bytes = new byte[DiskCache.PAGE_SIZE - offset - key.length * 8];
        }
        else{
            tmp_bytes = new byte[DiskCache.PAGE_SIZE - offset - (key.length + 1) * 8];
        }
        cursor.setOffset(offset);
        cursor.getBytes(tmp_bytes);
        cursor.setOffset(offset);
        for(long item : key){
            cursor.putLong(item);
        }
        if(!NodeHeader.isNodeWithSameLengthKeys(cursor)){
            cursor.putLong(-1l);
        }
        cursor.putBytes(tmp_bytes);

        NodeHeader.setNumberOfKeys(cursor, NodeHeader.getNumberOfKeys(cursor) + 1);

    }

    private static void insertChildAtIndex(PageCursor cursor, int index, long child){
        int childInsertionOffset = NodeHeader.NODE_HEADER_LENGTH + (index * 8);
        byte[] shiftDownBytes = new byte[DiskCache.PAGE_SIZE - childInsertionOffset - 8];
        cursor.setOffset(childInsertionOffset);
        cursor.getBytes(shiftDownBytes);
        cursor.setOffset(childInsertionOffset);
        cursor.putLong(child);
        cursor.putBytes(shiftDownBytes);
    }

    private static byte[] insertKeyAtIndex(byte[] keys, long[] newKey, int index, long[] returnedKey){
        LongBuffer keyB = ByteBuffer.wrap(keys).asLongBuffer();
        byte[] updatedKeys = new byte[keys.length + (newKey.length * 8)];
        ByteBuffer updatedKeysBB = ByteBuffer.wrap(updatedKeys);
        LongBuffer updatedKeysLB = updatedKeysBB.asLongBuffer();
        for(int i = 0; i < index; i++){
            for(int j = 0; j < newKey.length; j++) {
                updatedKeysLB.put(keyB.get());
            }
        }
        updatedKeysLB.put(newKey);
        int remaining = keyB.remaining();
        for(int i = 0; i < remaining; i++){
            updatedKeysLB.put(keyB.get());
        }

        int middle = updatedKeysLB.capacity()/2;
        for(int i = 0; i < returnedKey.length; i++){
            returnedKey[i] = updatedKeysLB.get(middle + i);
        }

        return updatedKeysBB.array();
    }

    private static byte[] insertChildAtIndex(byte[] children, long newChild, int index){
        LongBuffer childrenB = ByteBuffer.wrap(children).asLongBuffer();
        byte[] updatedChildren = new byte[children.length + 8];
        ByteBuffer updatedChildrenBB = ByteBuffer.wrap(updatedChildren);
        LongBuffer updatedChildrenLB = updatedChildrenBB.asLongBuffer();
        for(int i = 0; i < index; i++){
            updatedChildrenLB.put(childrenB.get());
        }
        updatedChildrenLB.put(newChild);
        int remaining = childrenB.remaining();
        for(int i = 0; i < remaining; i++){
            updatedChildrenLB.put(childrenB.get());
        }
        return updatedChildrenBB.array();
    }

    private static void updateHeader(PageCursor cursor, long[] key){
        if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
            if(NodeHeader.getNumberOfKeys(cursor) > 1 && NodeHeader.getKeyLength(cursor) != key.length){
                NodeHeader.setKeyLength(cursor, -1);
            }
            else if(NodeHeader.getNumberOfKeys(cursor) == 1){
                NodeHeader.setKeyLength(cursor, key.length);
            }
        }
    }

    private static void checkIfNodeRequiresDifferentLengthConversion(PageCursor cursor, long[] key){
        if(NodeHeader.isNodeWithSameLengthKeys(cursor) && NodeHeader.getNumberOfKeys(cursor) > 0 && NodeHeader.getKeyLength(cursor) != key.length){
            //Rewrite all the keys with delimiters there.
            int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
            int keySize = NodeHeader.getKeyLength(cursor);
            alignCursorToKeys(cursor);
            byte[] preDelimitedBytes = new byte[DiskCache.PAGE_SIZE - cursor.getOffset()];
            cursor.getBytes(preDelimitedBytes);
            alignCursorToKeys(cursor);
            LongBuffer buffer = ByteBuffer.wrap(preDelimitedBytes).asLongBuffer();
            for(int i = 0; i < numberOfKeys; i++){
                for(int j = 0; j < keySize; j++) {
                    cursor.putLong(buffer.get());
                }
                cursor.putLong(-1l);
            }
            NodeHeader.setKeyLength(cursor, -1);
        }
    }

    private static void alignCursorToKeys(PageCursor cursor){
        if(!NodeHeader.isLeafNode(cursor)){
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + (NodeHeader.getNumberOfKeys(cursor) + 1) * 8);
        }
        else{
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        }
    }
}
