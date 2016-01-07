/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package bptree.impl;

import bptree.PageProxyCursor;

import java.io.IOException;
import java.util.ArrayList;

import org.neo4j.io.pagecache.PagedFile;


public class InMemoryNode {
    public boolean leafNode = false;
    public long precedingNode = -1;
    public long followingNode = -1;
    public int keyLength;
    public int numberOfKeys;
    public ArrayList<Long> children = new ArrayList<>();
    public ArrayList<Long[]> keys = new ArrayList<>();

    public InMemoryNode(IndexTree tree, long id) throws IOException {
        PageProxyCursor cursor = tree.disk.getCursor(id, PagedFile.PF_EXCLUSIVE_LOCK);
        leafNode = NodeHeader.isLeafNode(cursor);
        precedingNode = NodeHeader.getPrecedingID(cursor);
        followingNode = NodeHeader.getSiblingID(cursor);
        if(NodeHeader.isLeafNode(cursor)){
            leafNodeSameLengthKeyDeserialization(cursor);
        }
        else{
            iNodeSameLengthKeyDeserialization(cursor);
        }
    }

    public InMemoryNode(PageProxyCursor cursor) throws IOException {
        leafNode = NodeHeader.isLeafNode(cursor);
        precedingNode = NodeHeader.getPrecedingID(cursor);
        followingNode = NodeHeader.getSiblingID(cursor);
        if(NodeHeader.isLeafNode(cursor)){
            leafNodeSameLengthKeyDeserialization(cursor);
        }
        else{
            iNodeSameLengthKeyDeserialization(cursor);
        }
    }

    protected void iNodeSameLengthKeyDeserialization(PageProxyCursor cursor){
        keyLength = NodeHeader.getKeyLength(cursor);
        numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        //ArrayList<Long> newKey = new ArrayList<>();
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        //Read all of the children id values
        //for(int i = 0; (numberOfKeys > 0) && i < (numberOfKeys + 1); i++){ //There is +1 children ids more than the number of keys
        for(int i = 0; i < (numberOfKeys + 1); i++){ //There is +1 children ids more than the number of keys
            children.add(cursor.getLong());
        }
        for(int i = 0; i < numberOfKeys; i++){
            Long[] newKey = new Long[keyLength];
            for(int j = 0; j < keyLength; j++){
                newKey[j] = (cursor.getLong());
            }
            //Now the variable newKey contains all the items in this key.
            //keys.add(newKey.toArray(new Long[newKey.size()]));
            keys.add(newKey);
            //newKey.clear(); //clear if for the next round.
        }
    }

    protected void leafNodeSameLengthKeyDeserialization(PageProxyCursor cursor){
        this.keyLength = NodeHeader.getKeyLength(cursor);
        this.numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        ArrayList<Long[]> deserialize_keys = new ArrayList<>();
        //LinkedList<Long> newKey = new LinkedList<>();
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        for(int i = 0; i < numberOfKeys; i++){
            Long[] newKey = new Long[keyLength];
            for(int j = 0; j < keyLength; j++){
                newKey[j] = (cursor.getLong());
            }
            //Now the variable newKey contains all the items in this key.
            //deserialize_keys.add(newKey.toArray(new Long[newKey.size()]));
            deserialize_keys.add(newKey);
            //newKey.clear(); //clear if for the next round.
        }
        this.keys = deserialize_keys;
    }

}
