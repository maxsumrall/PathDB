package bptree.impl;


import bptree.PageProxyCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;

/**
 * Created by max on 5/8/15.
 */
public class IndexDeletion {
    public static PageProxyCursor cursor;
    public static DiskCache disk;
    public IndexTree tree;

    public IndexDeletion(IndexTree tree){
        this.tree = tree;
    }

    public RemoveResultProxy remove(long[] key){
        RemoveResultProxy result = null;
        try (PageProxyCursor cursor = tree.disk.getCursor(tree.rootNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
                    if(NodeHeader.isLeafNode(cursor)){
                        result = removeKeyFromLeafNode(cursor, cursor.getCurrentPageId(), key);
                    } else {
                        int index = IndexSearch.search(cursor, key)[0];
                        long child = tree.getChildIdAtIndex(cursor, index);
                        long id = cursor.getCurrentPageId();
                        cursor.next(child);
                        result = remove(cursor, key);
                        if (result != null) {
                            cursor.next(id);
                            result = handleRemovedChildren(cursor, id, result);
                        }
                    }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
    private RemoveResultProxy remove(PageProxyCursor cursor, long[] key) throws IOException {
        RemoveResultProxy result = null;
        if(NodeHeader.isLeafNode(cursor)){
            result = removeKeyFromLeafNode(cursor, cursor.getCurrentPageId(), key);
        }
        else{
            int index = IndexSearch.search(cursor, key)[0];
            long child = tree.getChildIdAtIndex(cursor, index);
            long id = cursor.getCurrentPageId();
            cursor.next(child);
            result = remove(cursor, key);
            if(result != null){
                cursor.next(id);
                result = handleRemovedChildren(cursor, id, result);
            }
        }
        return result;
    }

    public static RemoveResultProxy handleRemovedChildren(PageProxyCursor cursor, long id, RemoveResultProxy result){
        int index = IndexTree.getIndexOfChild(cursor, result.removedNodeId);
        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        int numberOfChildren = numberOfKeys + 1;
        if(result.isLeaf){
            //Delete Child Pointer to deleted child
            //DELETE the key which divides/divided deleted child and mergedIntoChild
            if(index == numberOfKeys && numberOfKeys > 0 && numberOfChildren > 1){
                removeKeyAtIndex(cursor, (index -1)); //More than 1 child, but the right-most child is deleted.
            }
            else if(index < numberOfKeys){ // There exists a key to delete
                removeKeyAtIndex(cursor, index);
            }
            removeChildAtIndex(cursor, index);
        }
        else{//Internal Nodes
            //Delete Child Pointer to deleted child
            //DRAG (MOVE) the key which divides/divided deleted child and mergedIntoChild into mergedIntoChild.
            if(index == numberOfKeys && numberOfKeys > 0 && numberOfChildren > 1){
                removeKeyAtIndex(cursor, index -1); //More than 1 child, but the right-most child is deleted.
            }
            else if(index < numberOfKeys){ // There exists a key to delete
                removeKeyAtIndex(cursor, index);
            }
            removeChildAtIndex(cursor, index);
        }
        if(NodeHeader.getNumberOfKeys(cursor) == -1){
            result.removedNodeId = cursor.getCurrentPageId();
            result.siblingNodeID = NodeHeader.getSiblingID(cursor);
            result.isLeaf = false;
        }
        else{
            result = null;
        }
        return result;
    }

    public static RemoveResultProxy removeKeyAndChildFromInternalNode(PageProxyCursor cursor, long nodeId, long[] key, long child) throws IOException {
        RemoveResultProxy result = null;
        if(NodeHeader.getNumberOfKeys(cursor) == 1){
            result = new RemoveResultProxy(cursor.getCurrentPageId(), NodeHeader.getSiblingID(cursor), true);
            IndexTree.updateSiblingAndFollowingIdsDeletion(cursor, nodeId);
        }
        else{
            int[] searchResult = IndexSearch.search(cursor, key);
            removeKeyAtOffset(cursor, searchResult[1], key);
            removeChildAtIndex(cursor, searchResult[0]);
        }
        return result;
    }

    public RemoveResultProxy removeKeyFromLeafNode(long nodeId, long[] key){
        RemoveResultProxy result = null;
        try (PageProxyCursor cursor = tree.disk.getCursor(nodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
                    result = removeKeyFromLeafNode(cursor, nodeId, key);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static RemoveResultProxy removeKeyFromLeafNode(PageProxyCursor cursor, long nodeId, long[] key) throws IOException {
        RemoveResultProxy result = null;
        if(NodeHeader.getNumberOfKeys(cursor) == 1){
            result = new RemoveResultProxy(cursor.getCurrentPageId(), NodeHeader.getSiblingID(cursor), true);
            IndexTree.updateSiblingAndFollowingIdsDeletion(cursor, nodeId);
        }
        else{
            int[] searchResult = IndexSearch.search(cursor, key);
            removeKeyAtOffset(cursor, searchResult[1], key);
        }
        return result;
    }

    private static void removeKeyAtOffset(PageProxyCursor cursor, int offset, long[] key){
        byte[] tmp_bytes;

        tmp_bytes = new byte[cursor.capacity()- offset - key.length * 8];
        cursor.setOffset(offset + (key.length * 8));


        cursor.getBytes(tmp_bytes);
        cursor.setOffset(offset);

        cursor.putBytes(tmp_bytes);

        NodeHeader.setNumberOfKeys(cursor, NodeHeader.getNumberOfKeys(cursor) - 1);

    }

    private static void removeKeyAtIndex(PageProxyCursor cursor, int index){
        byte[] tmp_bytes;
        int offset;
        int nodeHeaderOffset = NodeHeader.NODE_HEADER_LENGTH + (NodeHeader.isLeafNode(cursor) ? 0 : (NodeHeader.getNumberOfKeys(cursor) + 1) * 8);
        int keyLength = NodeHeader.getKeyLength(cursor);
        offset = nodeHeaderOffset + (index * (keyLength * 8));
        tmp_bytes = new byte[cursor.capacity() - offset - keyLength * 8];
        cursor.setOffset(offset + (keyLength * 8));
        offset = cursor.getOffset();
        tmp_bytes = new byte[cursor.capacity() - offset - (keyLength + 1) * 8];
        long tmp = cursor.getLong();
        while (tmp != -1l) {
            cursor.getLong();
        }

        cursor.getBytes(tmp_bytes);
        cursor.setOffset(offset);

        cursor.putBytes(tmp_bytes);

        NodeHeader.setNumberOfKeys(cursor, NodeHeader.getNumberOfKeys(cursor) - 1);

    }

    public static void removeChildAtIndex(PageProxyCursor cursor, int index){
        byte[] tmp_bytes;
        int offset = NodeHeader.NODE_HEADER_LENGTH + (index * 8);
        tmp_bytes = new byte[cursor.capacity() - offset - 8];
        cursor.setOffset(offset + 8);

        cursor.getBytes(tmp_bytes);
        cursor.setOffset(offset);

        cursor.putBytes(tmp_bytes);
    }

    /*public static void addChildToSiblingNode(PageCursor cursor, RemoveResultProxy result, long childId) throws IOException {

        cursor.next(result.removedNodeId);
        long siblingId = NodeHeader.getSiblingID(cursor);
        long precedingNode = NodeHeader.getPrecedingID(cursor);

        if(siblingId != -1){//put it in sibling node
            cursor.next(siblingId);
            NodeTree.getChildIdAtIndex(cursor, 0);
            long[] firstKey =

        }
        else{//put it in preceding node

        }

    }
*/
}
