package bptree.impl;

import bptree.BulkLoadDataSource;
import bptree.PageProxyCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * Created by max on 5/8/15.
 */
public class NodeBulkLoader {

    private BulkLoadDataSource data;
    private PagedFile pagedFile;
    private DiskCache disk;
    public int keySize;
    public long finalLeafPage;
    public int MAX_PAIRS;
    private int RESERVED_CHILDREN_SPACE;
    private int currentPair = 0;
    private long currentParent;
    private int currentOffset = 0;
    private long previousLeaf = -1;
    private ParentBufferWriter parentWriter;
    public PageProxyCursor cursor;
    public NodeTree tree;

    public NodeBulkLoader(DiskCache disk, long finalPage, int keySize) throws IOException {
        this.disk = disk;
        this.finalLeafPage = finalPage;
        this.pagedFile = this.disk.pagedFile;
        AvailablePageIdPool.currentID = finalLeafPage;
        this.tree = new NodeTree(this.disk);
        this.keySize = keySize;
        this.MAX_PAIRS = ((DiskCache.PAGE_SIZE - NodeHeader.NODE_HEADER_LENGTH) / ((keySize + 1)*8) ) - 1;
        this.RESERVED_CHILDREN_SPACE  = (MAX_PAIRS + 1) * 8;
        parentWriter = new ParentBufferWriter();
    }

    public long run(){
        long root = -1;
        try (PageProxyCursor cursor = this.disk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK)) {
                    long firstInternalNode = NodeTree.acquireNewInternalNode(cursor);
                    cursor.next(firstInternalNode);
                    NodeHeader.setKeyLength(cursor, keySize);
                    this.currentParent = firstInternalNode;
                    //while(data.hasNext()){
                    //    insertKeys(cursor, data.nextPage());
                    //}
                    for(int i = 0; i < finalLeafPage; i++){
                        addLeafToParent(cursor, i);
                    }
                    cursor.next(currentParent);
                    cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                    cursor.putBytes(parentWriter.getChildren());
                    byte[] keys = parentWriter.getKeys();
                    cursor.putBytes(keys);
                    NodeHeader.setNumberOfKeys(cursor, ((keys.length/ keySize) / 8));
                    //Leaf row and one parent row made.
                    //Build tree above internal nodes.
                    root = buildUpperLeaves(cursor, firstInternalNode);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return root;
    }

    private void insertKeys(PageProxyCursor cursor, byte[] keyBytes) throws IOException {
        long newLeaf = NodeTree.acquireNewLeafNode(cursor);
        cursor.next(newLeaf);
        NodeHeader.setKeyLength(cursor, keySize);
        NodeHeader.setNumberOfKeys(cursor, keyBytes.length/(keySize * 8));
        writeBytesToLeaf(cursor, newLeaf, keyBytes);
        if(previousLeaf != -1) {
            NodeTree.updateSiblingAndFollowingIdsInsertion(cursor, previousLeaf, newLeaf);
        }
        addLeafToParent(cursor, newLeaf);
        this.previousLeaf = newLeaf;
    }

    private void writeBytesToLeaf(PageProxyCursor cursor, long leaf, byte[] bytes) throws IOException {
        cursor.next(leaf);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        cursor.putBytes(bytes);
    }

    private void addLeafToParent(PageProxyCursor cursor, long leaf) throws IOException {
        if(currentPair > MAX_PAIRS){
            cursor.next(this.currentParent);
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            cursor.putBytes(parentWriter.getChildren());
            cursor.putBytes(parentWriter.getKeys());
            NodeHeader.setNumberOfKeys(cursor, MAX_PAIRS);
            long newParent = NodeTree.acquireNewInternalNode(cursor);
            cursor.next(newParent);
            NodeHeader.setKeyLength(cursor, keySize);
            NodeTree.updateSiblingAndFollowingIdsInsertion(cursor, this.currentParent, newParent);
            this.currentParent = newParent;
            this.currentOffset = 0;
            this.currentPair = 0;
        }
        if(this.currentOffset == 0){
            parentWriter.addChild(leaf);
        }
        else{
            cursor.next(leaf);
            parentWriter.addChild(leaf);
            parentWriter.addKey(NodeInsertion.getFirstKeyInNodeAsBytes(cursor));
        }
        this.currentPair++;
        this.currentOffset+=8;

    }

    private long buildUpperLeaves(PageProxyCursor cursor, long leftMostNode) throws IOException {
        long firstParent = NodeTree.acquireNewInternalNode(cursor);
        cursor.next(firstParent);
        NodeHeader.setKeyLength(cursor, keySize);
        this.currentParent = firstParent;
        this.currentOffset = 0;
        this.currentPair = 0;
        long currentNode = leftMostNode;
        cursor.next(leftMostNode);
        long nextNode = NodeHeader.getSiblingID(cursor);

        while(nextNode != -1l){
            copyUpLeafToParent(cursor, currentNode);
            currentNode = nextNode;
            cursor.next(nextNode);
            nextNode = NodeHeader.getSiblingID(cursor);
        }
        copyUpLeafToParent(cursor, currentNode);
        cursor.next(currentParent);
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        cursor.putBytes(parentWriter.getChildren());
        byte[] keys = parentWriter.getKeys();
        cursor.putBytes(keys);
        NodeHeader.setNumberOfKeys(cursor, ((keys.length/ keySize) / 8));

        if(firstParent != this.currentParent){
            return buildUpperLeaves(cursor, firstParent);
        }

        else{
            return firstParent;
        }
    }

    private void copyUpLeafToParent(PageProxyCursor cursor, long leaf) throws IOException {
        if(currentPair > MAX_PAIRS){
            cursor.next(this.currentParent);
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            cursor.putBytes(parentWriter.getChildren());
            cursor.putBytes(parentWriter.getKeys());
            NodeHeader.setNumberOfKeys(cursor, MAX_PAIRS);
            long newParent = NodeTree.acquireNewInternalNode(cursor);
            cursor.next(newParent);
            NodeHeader.setKeyLength(cursor, keySize);
            NodeTree.updateSiblingAndFollowingIdsInsertion(cursor, this.currentParent, newParent);
            this.currentParent = newParent;
            this.currentOffset = 0;
            this.currentPair = 0;
        }
        if(this.currentOffset == 0){
            parentWriter.addChild(leaf);
        }
        else{
            cursor.next(leaf);
            parentWriter.addChild(leaf);
            parentWriter.addKey(traverseToFindFirstKeyInLeafAsBytes(cursor));
        }
        this.currentPair++;
        this.currentOffset+=8;
    }
    public byte[] traverseToFindFirstKeyInLeafAsBytes(PageProxyCursor cursor) throws IOException {
        if(NodeHeader.isLeafNode(cursor)){
            return NodeInsertion.getFirstKeyInNodeAsBytes(cursor);
        }
        else{
            long leftMostChild = tree.getChildIdAtIndex(cursor, 0);
            cursor.next(leftMostChild);
            return traverseToFindFirstKeyInLeafAsBytes(cursor);
        }
    }


    private class ParentBufferWriter {
        byte[] children = new byte[RESERVED_CHILDREN_SPACE];
        byte[] keys = new byte[MAX_PAIRS * keySize * 8];
        ByteBuffer cb = ByteBuffer.wrap(children);
        LongBuffer cBuffer = cb.asLongBuffer();
        ByteBuffer kb = ByteBuffer.wrap(keys);

        void addChild(long child) {
            cBuffer.put(child);
        }
        void addKey(byte[] key){
            kb.put(key);
        }

        byte[] getChildren(){
            int index = cBuffer.position();
            cBuffer.position(0);
            if(index != cBuffer.limit()){
                byte[] partialBytes = new byte[index * 8];
                System.arraycopy(children, 0, partialBytes, 0, partialBytes.length);
                return partialBytes;
            }
            return children;
        }
        byte[] getKeys(){
            int index = kb.position();
            kb.position(0);
            if(index != kb.limit()){
                byte[] partialBytes = new byte[index];
                System.arraycopy(keys, 0, partialBytes, 0, partialBytes.length);
                return partialBytes;
            }
            return keys;
        }
    }
}
