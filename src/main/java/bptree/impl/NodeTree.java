package bptree.impl;

import bptree.PageProxyCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;

/**
 * Static class for manipulating nodes without doing any object instantiation.
 */
public class NodeTree {

    public static PagedFile pagedFile;
    public static KeyImpl comparator = new KeyImpl();
    public static long rootNodeId = 0;
    public static PageProxyCursor cursor;

    public NodeTree(long rootNodeId, PagedFile pagedFile){
        NodeTree.rootNodeId = rootNodeId;
        NodeTree.pagedFile = pagedFile;
    }

    public NodeTree(PagedFile pagedFile) throws IOException {
        NodeTree.pagedFile = pagedFile;
        NodeTree.rootNodeId = acquireNewLeafNode();
    }

    public static void setPagedFile(PagedFile pagedFile){
        NodeTree.pagedFile = pagedFile;
    }

    public static void newRoot(long childA, long childB, long[] key){
        try (PageProxyCursor cursor = DiskCache.getCursor(rootNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            cursor.putLong(childA);
            cursor.putLong(childB);
            for(int i = 0; i < key.length; i++){
                cursor.putLong(key[i]);
            }
            NodeHeader.setKeyLength(cursor, key.length);
            NodeHeader.setNumberOfKeys(cursor, 1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public SearchCursor find(long[] key){
        return NodeSearch.find(key);
    }

    public void insert(long[] key){
        NodeInsertion.insert(key);
    }

    public void remove(long[] key){
        NodeDeletion.remove(key);
    }



    public static void setPrecedingId(long nodeId, long newPrecedingId){
        try (PageProxyCursor cursor = DiskCache.getCursor(rootNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
                NodeHeader.setPrecedingId(cursor, newPrecedingId);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void setFollowingId(long nodeId, long newFollowingId){
        try (PageProxyCursor cursor = DiskCache.getCursor(rootNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
                    NodeHeader.setFollowingID(cursor, newFollowingId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static long getChildIdAtIndex(PageProxyCursor cursor, int indexOfChild){
        long childId = 0;
        childId = cursor.getLong(NodeHeader.NODE_HEADER_LENGTH + indexOfChild * 8);
        return childId;
    }

    public static long getChildIdAtIndex(long nodeId, int indexOfChild){
        long childId = 0;
        try (PageProxyCursor cursor = DiskCache.getCursor(rootNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
                    childId = getChildIdAtIndex(cursor, indexOfChild);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return childId;
    }


    public static int getIndexOfChild(long nodeId, long childId){
        int childIndex = -1;
        try (PageProxyCursor cursor = DiskCache.getCursor(rootNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
                    childIndex = getIndexOfChild(cursor, childId);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return childIndex;
    }

    public static int getIndexOfChild(PageProxyCursor cursor, long childId){
        int childIndex = 0;
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        long child = cursor.getLong();
        while(child != childId){
            child = cursor.getLong();
            childIndex++;
        }
        return childIndex;
    }


    public static void updateSiblingAndFollowingIdsInsertion(PageProxyCursor cursor, long nodeId, long newNodeId) throws IOException {
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
    public static void updateSiblingAndFollowingIdsDeletion(PageProxyCursor cursor, long nodeId) throws IOException {
        cursor.next(nodeId);
        long following = NodeHeader.getSiblingID(cursor);
        long preceding = NodeHeader.getPrecedingID(cursor);
        if(following != -1l) {
            cursor.next(following);
            NodeHeader.setPrecedingId(cursor, preceding);
        }
        if(preceding!= -1l) {
            cursor.next(preceding);
            NodeHeader.setFollowingID(cursor, following);
        }
    }

    public static long acquireNewLeafNode(PageProxyCursor cursor) throws IOException {
        long newNodeId = AvailablePageIdPool.acquireId();
        cursor.next(newNodeId);
        NodeHeader.initializeLeafNode(cursor);
        return newNodeId;
    }
    public static long acquireNewLeafNode() throws IOException {
        long newNodeId = AvailablePageIdPool.acquireId();
        try(PageProxyCursor cursor = DiskCache.getCursor(newNodeId, PagedFile.PF_EXCLUSIVE_LOCK)) {
            NodeHeader.initializeLeafNode(cursor);
        }
        catch(Exception e ){

        }
        return newNodeId;
    }

    public static long acquireNewInternalNode(PageProxyCursor cursor) throws IOException {
        long newNodeId = AvailablePageIdPool.acquireId();
        cursor.next(newNodeId);
        NodeHeader.initializeInternalNode(cursor);
        return newNodeId;
    }

    public static void releaseNode(long nodeId){
        AvailablePageIdPool.releaseId(nodeId);
    }

    public static void removeFirstKeyInInternalNode(PageProxyCursor cursor){
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
}
