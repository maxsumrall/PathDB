/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package bptree.impl;

import bptree.PageProxyCursor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import org.neo4j.io.pagecache.PagedFile;


public class IndexBulkLoader {

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
    public IndexTree tree;

    public IndexBulkLoader(DiskCache disk, long finalPage, int keySize) throws IOException {
        this.disk = disk;
        this.finalLeafPage = finalPage;
        TreeNodeIDManager.currentID = finalLeafPage + 1;
        this.tree = new IndexTree(keySize, 0, this.disk);
        this.keySize = keySize;
        this.MAX_PAIRS = ((DiskCache.PAGE_SIZE - NodeHeader.NODE_HEADER_LENGTH) / ((keySize + 1)*8) ) - 1;
        this.RESERVED_CHILDREN_SPACE  = (MAX_PAIRS + 1) * 8;
        parentWriter = new ParentBufferWriter();
    }

    public IndexTree run() throws IOException {
        long root;
        try (PageProxyCursor cursor = this.disk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK)) {
            long firstInternalNode = IndexTree.acquireNewInternalNode(cursor);
            cursor.next(firstInternalNode);
            NodeHeader.setKeyLength(cursor, keySize);
            this.currentParent = firstInternalNode;

            for (int i = 0; i < finalLeafPage; i++) {
                addLeafToParent(cursor, i);
            }
            cursor.next(currentParent);
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            byte[] children = parentWriter.getChildren();
            cursor.putBytes(children);
            byte[] keys = parentWriter.getKeys();
            cursor.putBytes(keys);
            NodeHeader.setNumberOfKeys(cursor, ((keys.length / keySize) / 8));
            //Leaf row and one parent row made.
            //Build tree above internal nodes.
            root = buildUpperLeaves(cursor, firstInternalNode);
            tree.rootNodeId = root;
        }
        return tree;
    }


    private void addLeafToParent(PageProxyCursor cursor, long leaf) throws IOException {
        if(currentPair > MAX_PAIRS){
            cursor.next(this.currentParent);
            cursor.deferWriting();
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            cursor.putBytes(parentWriter.getChildren());
            cursor.putBytes(parentWriter.getKeys());
            NodeHeader.setNumberOfKeys(cursor, MAX_PAIRS);
            cursor.resumeWriting();
            long newParent = IndexTree.acquireNewInternalNode(cursor);
            cursor.next(newParent);
            NodeHeader.setKeyLength(cursor, keySize);
            IndexTree.updateSiblingAndFollowingIdsInsertion(cursor, this.currentParent, newParent);
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
            parentWriter.addKey(IndexInsertion.getFirstKeyInNodeAsBytes(cursor));
        }
        this.currentPair++;
        this.currentOffset+=8;

    }

    private long buildUpperLeaves(PageProxyCursor cursor, long leftMostNode) throws IOException {
        long firstParent = IndexTree.acquireNewInternalNode(cursor);
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
        cursor.deferWriting();
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        cursor.putBytes(parentWriter.getChildren());
        byte[] keys = parentWriter.getKeys();
        cursor.putBytes(keys);
        NodeHeader.setNumberOfKeys(cursor, ((keys.length/ keySize) / 8));
        cursor.resumeWriting();

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
            cursor.deferWriting();
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            cursor.putBytes(parentWriter.getChildren());
            cursor.putBytes(parentWriter.getKeys());
            NodeHeader.setNumberOfKeys(cursor, MAX_PAIRS);
            cursor.resumeWriting();
            long newParent = IndexTree.acquireNewInternalNode(cursor);
            cursor.next(newParent);
            NodeHeader.setKeyLength(cursor, keySize);
            IndexTree.updateSiblingAndFollowingIdsInsertion(cursor, this.currentParent, newParent);
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
            return IndexInsertion.getFirstKeyInNodeAsBytes(cursor);
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
