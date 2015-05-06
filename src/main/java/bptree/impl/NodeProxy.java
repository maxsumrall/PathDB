package bptree.impl;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

/**
 * Static class for manipulating nodes without doing any object instantiation.
 */
public class NodeProxy {

    public PagedFile pagedFile;
    public static KeyImpl comparator = new KeyImpl();
    private PrimitiveLongArray arrayUtil = new PrimitiveLongArray();
    public long rootNodeId = 0;

    public NodeProxy(long rootNodeId, PagedFile pagedFile){
        this.rootNodeId = rootNodeId;
        this.pagedFile = pagedFile;
    }

    public void setPagedFile(PagedFile pagedFile){
        this.pagedFile = pagedFile;
    }

    public void newRoot(long childA, long childB, long[] key){
        try (PageCursor cursor = pagedFile.io(rootNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                    cursor.putLong(childA);
                    cursor.putLong(childB);
                    for(int i = 0; i < key.length; i++){
                        cursor.putLong(key[i]);
                    }
                    NodeHeader.setKeyLength(cursor, key.length);
                    NodeHeader.setNumberOfKeys(cursor, 1);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SplitResult insert(long[] key){
        SplitResult result = null;
        try (PageCursor cursor = pagedFile.io(rootNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    if(NodeHeader.isLeafNode(cursor)){
                        result = addKeyToLeafNode(cursor, cursor.getCurrentPageId(), key);
                    }
                    else{
                        int index = search(cursor, key)[0];
                        long child = getChildIdAtIndex(cursor, index);
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
    private SplitResult insert(PageCursor cursor, long[] key) throws IOException {
        SplitResult result = null;
        if(NodeHeader.isLeafNode(cursor)){
            result = addKeyToLeafNode(cursor, cursor.getCurrentPageId(), key);
        }
        else{
            int index = search(cursor, key)[0];
            long child = getChildIdAtIndex(cursor, index);
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

    public long getChildIdAtIndex(PageCursor cursor, int indexOfChild){
        long childId = 0;
        childId = cursor.getLong(NodeHeader.NODE_HEADER_LENGTH + indexOfChild * 8);
        return childId;
    }

    public long getChildIdAtIndex(long nodeId, int indexOfChild){
        long childId = 0;
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_SHARED_LOCK)) {
            if (cursor.next()) {
                do {
                    childId = getChildIdAtIndex(cursor, indexOfChild);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return childId;
    }

    public SplitResult addKeyAndChildToInternalNode(long nodeId, long[] key, long child){
        SplitResult result = null;
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
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

    public SplitResult addKeyAndChildToInternalNode(PageCursor cursor, long nodeId, long[] key, long child) throws IOException {
        SplitResult result = null;
        if(!internalNodeContainsSpaceForNewKeyAndChild(cursor, key)){
            long newInternalNodeId = acquireNewInternalNode(cursor);
            result = new SplitResult();
            result.left = nodeId;
            result.right = newInternalNodeId;
            updateSiblingAndFollowingIds(cursor, nodeId, newInternalNodeId);
            result.primkey = insertAndBalanceKeysBetweenInternalNodes(cursor, nodeId, newInternalNodeId, key, child);
            if(!newKeyBelongsInNewNode(cursor, key)){
                cursor.next(nodeId);
            }
        }
        else{
            checkIfNodeRequiresDifferentLengthConversion(cursor, key);
            int[] searchResult = search(cursor, key);
            insertKeyAtIndex(cursor, searchResult[1], key);
            insertChildAtIndex(cursor, searchResult[0] + 1, child);
            updateHeader(cursor, key);
        }
        return result;
    }

    public SplitResult addKeyToLeafNode(long nodeId, long[] key){
        SplitResult result = null;
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
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

    public SplitResult addKeyToLeafNode(PageCursor cursor, long nodeId, long[] key) throws IOException {
        SplitResult result = null;
        if(!leafNodeContainsSpaceForNewKey(cursor, key)){
            long newLeafNodeId = acquireNewLeafNode(cursor);
            result = new SplitResult();
            result.left = nodeId;
            result.right = newLeafNodeId;
            updateSiblingAndFollowingIds(cursor, nodeId, newLeafNodeId);
            result.primkey = insertAndBalanceKeysBetweenLeafNodes(cursor, nodeId, newLeafNodeId, key);
        }
        else{
            checkIfNodeRequiresDifferentLengthConversion(cursor, key);
            int[] searchResult = search(cursor, key);
            insertKeyAtIndex(cursor, searchResult[1], key);
            updateHeader(cursor, key);
        }
        return result;
    }
    private void updateSiblingAndFollowingIds(PageCursor cursor, long nodeId, long newNodeId) throws IOException {
        cursor.next(nodeId);
        long oldFollowing = NodeHeader.getSiblingID(cursor);
        NodeHeader.setFollowingID(cursor, newNodeId);
        if(oldFollowing != -1l) {
            cursor.next(oldFollowing);
            NodeHeader.setPrecedingId(cursor, newNodeId);
        }
        cursor.next(newNodeId);
        NodeHeader.setFollowingID(cursor, oldFollowing);
        NodeHeader.setPrecedingId(cursor, nodeId);
    }

    private long acquireNewLeafNode(PageCursor cursor) throws IOException {
        long newNodeId = AvailablePageIdPool.acquireId();
        cursor.next(newNodeId);
        NodeHeader.initializeLeafNode(cursor);
        return newNodeId;
    }

    public long acquireNewInternalNode(PageCursor cursor) throws IOException {
        long newNodeId = AvailablePageIdPool.acquireId();
        cursor.next(newNodeId);
        NodeHeader.initializeInternalNode(cursor);
        return newNodeId;
    }

    private long[] insertAndBalanceKeysBetweenLeafNodes(PageCursor cursor, long fullNode, long emptyNode, long[] newKey) throws IOException {
        //grab half of the keys from the first node, dump into the new node.
        cursor.next(fullNode);
        long[] returnedKey = null;
        byte[] keysA = null;
        byte[] keysB = null;

        int[] searchResults = search(cursor, newKey);
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

    private long[] insertAndBalanceKeysBetweenInternalNodes(PageCursor cursor, long fullNode, long emptyNode, long[] newKey, long newChild) throws IOException {
        //grab half of the keys from the first node, dump into the new node.
        cursor.next(fullNode);
        long[] returnedKey = null;
        byte[] childrenA = null;
        byte[] childrenB = null;
        byte[] keysA = null;
        byte[] keysB = null;

        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);

        int[] searchResults = search(cursor, newKey);
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

    private boolean newKeyBelongsInNewNode(PageCursor cursor, long[] newKey){
        return comparator.prefixCompare(newKey, getFirstKeyInNode(cursor)) > 0;

    }
    private long[] getFirstKeyInNode(PageCursor cursor){
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

    private void removeFirstKeyInInternalNode(PageCursor cursor){
        byte[] compactionBytes = new byte[DiskCache.PAGE_SIZE - NodeHeader.NODE_HEADER_LENGTH - 8]; //removing child
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + 8);
        cursor.getBytes(compactionBytes);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        cursor.putBytes(compactionBytes);

        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
            int keyLength = NodeHeader.getKeyLength(cursor);
            compactionBytes = new byte[DiskCache.PAGE_SIZE - NodeHeader.NODE_HEADER_LENGTH - (numberOfKeys * 8) - (8 * keyLength)];
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + (numberOfKeys * 8) + (8 * keyLength));
            cursor.getBytes(compactionBytes);
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + (numberOfKeys * 8));
            cursor.putBytes(compactionBytes);
        }
        else{
            long currKey = cursor.getLong();
            while(currKey != Node.KEY_DELIMITER) {
                currKey = cursor.getLong();
            }
            int endOfFirstKeyPos = cursor.getOffset();
            compactionBytes = new byte[DiskCache.PAGE_SIZE - endOfFirstKeyPos];
            cursor.getBytes(compactionBytes);
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + (numberOfKeys * 8));
            cursor.putBytes(compactionBytes);
        }
        NodeHeader.setNumberOfKeys(cursor, numberOfKeys - 1);
    }

    private void insertKeyAtIndex(PageCursor cursor, int offset, long[] key){
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

    private void insertChildAtIndex(PageCursor cursor, int index, long child){
        int childInsertionOffset = NodeHeader.NODE_HEADER_LENGTH + (index * 8);
        byte[] shiftDownBytes = new byte[DiskCache.PAGE_SIZE - childInsertionOffset - 8];
        cursor.setOffset(childInsertionOffset);
        cursor.getBytes(shiftDownBytes);
        cursor.setOffset(childInsertionOffset);
        cursor.putLong(child);
        cursor.putBytes(shiftDownBytes);
    }

    private byte[] insertKeyAtIndex(byte[] keys, long[] newKey, int index, long[] returnedKey){
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

    private byte[] insertChildAtIndex(byte[] children, long newChild, int index){
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

    private void updateHeader(PageCursor cursor, long[] key){
        if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
            if(NodeHeader.getNumberOfKeys(cursor) > 1 && NodeHeader.getKeyLength(cursor) != key.length){
                NodeHeader.setKeyLength(cursor, -1);
            }
            else if(NodeHeader.getNumberOfKeys(cursor) == 1){
                NodeHeader.setKeyLength(cursor, key.length);
            }
        }
    }

    private void checkIfNodeRequiresDifferentLengthConversion(PageCursor cursor, long[] key){
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

    private void alignCursorToKeys(PageCursor cursor){
        if(!NodeHeader.isLeafNode(cursor)){
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH + (NodeHeader.getNumberOfKeys(cursor) + 1) * 8);
        }
        else{
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        }
    }

    public ProxyCursor find(long[] key){
        long[] entry = null;
        ProxyCursor resultsCursor = null;
        int[] searchResult;
        try (PageCursor cursor = pagedFile.io(rootNodeId, PagedFile.PF_SHARED_LOCK)) {
            if (cursor.next()) {
                do {
                    searchResult = find(cursor, key);
                    long currentNode = cursor.getCurrentPageId();
                    int[] altResult = moveCursorBackIfPreviousNodeContainsValidKeys(cursor, key);
                    if(currentNode != cursor.getCurrentPageId()){
                        searchResult = altResult;
                    }
                    byte[] keys = new byte[NodeHeader.getNumberOfKeys(cursor) * NodeHeader.getKeyLength(cursor) * 8];
                    cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                    cursor.getBytes(keys);
                    LongBuffer keysLB = ByteBuffer.wrap(keys).asLongBuffer();
                    resultsCursor = new ProxyCursor(NodeHeader.getSiblingID(cursor), searchResult[0], keysLB, key, NodeHeader.getKeyLength(cursor), this);
                }
                while (cursor.shouldRetry());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultsCursor;
    }

    public int[] find(PageCursor cursor, long[] key) throws IOException {
        int[] searchResult;
        if(NodeHeader.isLeafNode(cursor)){
            searchResult = search(cursor, key);
        }
        else{
            int index = search(cursor, key)[0];
            long child = getChildIdAtIndex(cursor, index);
            cursor.next(child);
            searchResult = find(cursor, key);
        }
        return searchResult;
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
            for(int j = 0; j < keyLength; j++) {
                currKey[j] = cursor.getLong();
            }
            if(comparator.prefixCompare(key, currKey) <= 0){
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
            if(comparator.prefixCompare(key, arrayUtil.get()) == 0){
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

    public boolean leafNodeContainsSpaceForNewKey(PageCursor cursor, long[] newKey){
        return leafNodeByteSize(cursor, newKey) < DiskCache.PAGE_SIZE;
    }

    public int leafNodeByteSize(long nodeId, long[] newKey){
        int size = 0;
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
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

    public int leafNodeByteSize(PageCursor cursor, long[] newKey){
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

    public boolean internalNodeContainsSpaceForNewKeyAndChild(PageCursor cursor, long[] newKey){
        return internalNodeByteSize(cursor, newKey) < DiskCache.PAGE_SIZE;
    }

    public int internalNodeByteSize(long nodeId, long[] newKey){
        int size = 0;
        try (PageCursor cursor = pagedFile.io(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
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

    public int internalNodeByteSize(PageCursor cursor, long[] newKey){
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

    private int[] moveCursorBackIfPreviousNodeContainsValidKeys(PageCursor cursor, long[] key) throws IOException {
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
