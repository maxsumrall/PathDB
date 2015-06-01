package bptree.impl;

import bptree.PageProxyCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.util.ArrayList;


public class InMemoryNode {
    public boolean leafNode = false;
    public long precedingNode = -1;
    public long followingNode = -1;
    public int keyLength;
    public int numberOfKeys;
    public ArrayList<Long> children = new ArrayList<>();
    public ArrayList<Long[]> keys = new ArrayList<>();

    public InMemoryNode(NodeTree tree, long id) throws IOException {
        PageProxyCursor cursor = tree.disk.getCursor(id, PagedFile.PF_EXCLUSIVE_LOCK);
        leafNode = NodeHeader.isLeafNode(cursor);
        precedingNode = NodeHeader.getPrecedingID(cursor);
        followingNode = NodeHeader.getSiblingID(cursor);
        if(NodeHeader.isLeafNode(cursor)){
            if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
                leafNodeSameLengthKeyDeserialization(cursor);
            }
            else{
                leafNodeVariableLengthKeyDeserialization(cursor);
            }
        }
        else{
            if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
                iNodeSameLengthKeyDeserialization(cursor);
            }
            else{
                iNodeVariableLengthKeyDeserialization(cursor);
            }
        }
    }

    public InMemoryNode(PageProxyCursor cursor) throws IOException {
        leafNode = NodeHeader.isLeafNode(cursor);
        precedingNode = NodeHeader.getPrecedingID(cursor);
        followingNode = NodeHeader.getSiblingID(cursor);
        if(NodeHeader.isLeafNode(cursor)){
            if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
                leafNodeSameLengthKeyDeserialization(cursor);
            }
            else{
                leafNodeVariableLengthKeyDeserialization(cursor);
            }
        }
        else{
            if(NodeHeader.isNodeWithSameLengthKeys(cursor)){
                iNodeSameLengthKeyDeserialization(cursor);
            }
            else{
                iNodeVariableLengthKeyDeserialization(cursor);
            }
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

    protected void iNodeVariableLengthKeyDeserialization(PageProxyCursor cursor){
        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        ArrayList<Long> newKey = new ArrayList<>();
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        //Read all of the children id values
        for(int i = 0; i < numberOfKeys + 1; i++){ //There is +1 children ids more than the number of keys
            children.add(cursor.getLong());
        }
        Long nextValue = cursor.getLong();
        for(int i = 0; (numberOfKeys > 0) && (i < numberOfKeys); i++){ //check if we are at the final end of the block
            while(nextValue != NodeHeader.KEY_DELIMITER) { //while still within this key
                newKey.add(nextValue);
                nextValue = cursor.getLong();
            }
            keys.add(newKey.toArray(new Long[newKey.size()]));
            newKey.clear();
            if(i + 1 < numberOfKeys) {
                nextValue = cursor.getLong();//Without this check, there are problems for the last value being at the very end of the block.
            }
        }
    }

    protected void leafNodeSameLengthKeyDeserialization(PageProxyCursor cursor){
        int keyLength = NodeHeader.getKeyLength(cursor);
        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
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

    protected void leafNodeVariableLengthKeyDeserialization(PageProxyCursor cursor){
        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        ArrayList<Long[]> deserialize_keys = new ArrayList<>();
        ArrayList<Long> newKey = new ArrayList<>();
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        Long nextValue = cursor.getLong();
        for(int i = 0; i < numberOfKeys; i++){ //check if we are at the final end of the block
            while(nextValue != NodeHeader.KEY_DELIMITER) { //while still within this key
                newKey.add(nextValue);
                nextValue = cursor.getLong();
            }
            deserialize_keys.add(newKey.toArray(new Long[newKey.size()]));
            newKey.clear();
            if(i + 1 < numberOfKeys) {
                nextValue = cursor.getLong();//Without this check, there are problems for the last value being at the very end of the block.
            }
        }
        this.keys = deserialize_keys;
    }

}
