package bptree;

import bptree.impl.InternalNode;
import bptree.impl.Node;
import bptree.impl.PathIndexImpl;
import bptree.impl.Tree;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static bptree.LabelsAndPathsGenerator.*;

/**
 * Created by max on 3/24/15.
 */
public class DeletetionTest {
    private Index index;
    private ArrayList<Long[]> labelPaths;

    @Before
    public void initializeIndex() throws IOException {
        labelPaths = exampleLabelPaths(20,2);
        index = PathIndexImpl.getTemporaryPathIndex()
                .setRangeOfPathLengths(2, 2)
                .setLabelPaths(labelPaths)
                .setSignaturesToDefault();
    }

    @Test
    public void testInsertSequentialKeysIntoIndexSmall() throws IOException {
        int number_of_keys_to_insert = 50;
        ArrayList<Key> keys = exampleSequentialKeys(labelPaths, number_of_keys_to_insert);
        for(Key key: keys){
            index.insert(key);
        }
        Cursor cursor;
        for(Key key : keys){
            cursor = index.find(key);
            assert(cursor.hasNext());
            assert(Arrays.equals(cursor.next(), index.buildComposedKey(key))); //the empty set
        }
        for(int i  = 0; i < keys.size(); i++){
            RemoveResult removeResult = index.remove(keys.get(i));
            assert(removeResult.getN() == 1);
            cursor = index.find(keys.get(i));
            assert(!cursor.hasNext());
            assert(leafNodesAreConsistent(((PathIndexImpl) index).tree));
        }
    }

    @Test
    public void testInsertRandomKeysIntoIndexSmall() throws IOException {
        int number_of_keys_to_insert = 50;
        ArrayList<Key> keys = exampleRandomKeys(labelPaths, number_of_keys_to_insert);
        for(Key key: keys){
            index.insert(key);
        }
        Cursor cursor;
        for(Key key : keys){
            cursor = index.find(key);
            assert(cursor.hasNext());
            assert(Arrays.equals(cursor.next(), index.buildComposedKey(key))); //the empty set
        }
        for(int i  = 0; i < keys.size(); i++){
            RemoveResult removeResult = index.remove(keys.get(i));
            assert(removeResult.getN() == 1);
            cursor = index.find(keys.get(i));
            assert(!cursor.hasNext());
            assert(leafNodesAreConsistent(((PathIndexImpl) index).tree));
        }
    }

    @Test
    public void testInsertSequentialKeysIntoIndex() throws IOException {
        int number_of_keys_to_insert = 3000;
        ArrayList<Key> keys = exampleSequentialKeys(labelPaths, number_of_keys_to_insert);
        for(Key key: keys){
            index.insert(key);
        }
        Cursor cursor;
        for(Key key : keys){
            cursor = index.find(key);
            assert(cursor.hasNext());
            assert(Arrays.equals(cursor.next(), index.buildComposedKey(key))); //the empty set
        }
        for(int i  = 0; i < keys.size(); i++){
            RemoveResult removeResult = index.remove(keys.get(i));
            assert(removeResult.getN() == 1);
            cursor = index.find(keys.get(i));
            assert(!cursor.hasNext());
            assert(leafNodesAreConsistent(((PathIndexImpl) index).tree));
        }
    }

    @Test
    public void testInsertRandomKeysIntoIndex() throws IOException {
        int number_of_keys_to_insert = 5000;
        ArrayList<Key> keys = exampleRandomKeys(labelPaths, number_of_keys_to_insert);
        for(Key key: keys){
            index.insert(key);
        }
        Cursor cursor;
        for(Key key : keys){
            cursor = index.find(key);
            assert(cursor.hasNext());
            assert(Arrays.equals(cursor.next(), index.buildComposedKey(key))); //the empty set
        }
        for(int i  = 0; i < keys.size(); i++){
            RemoveResult removeResult = index.remove(keys.get(i));
            assert(removeResult.getN() == 1);
            cursor = index.find(keys.get(i));
            assert(!cursor.hasNext());
            assert(leafNodesAreConsistent(((PathIndexImpl) index).tree));
        }
    }
    @Test
    public void testEnsureNoEmptyNodesRemain() throws IOException {
        int number_of_keys_to_insert = 5000;
        ArrayList<Key> keys = exampleRandomKeys(labelPaths, number_of_keys_to_insert);
        for(Key key: keys){
            index.insert(key);
        }
        Cursor cursor;
        for(Key key : keys){
            cursor = index.find(key);
            assert(cursor.hasNext());
            assert(Arrays.equals(cursor.next(), index.buildComposedKey(key))); //the empty set
        }
        for(int i  = 0; i < keys.size(); i++){
            RemoveResult removeResult = index.remove(keys.get(i));
            assert(removeResult.getN() == 1);
            cursor = index.find(keys.get(i));
            assert(!cursor.hasNext());
            assert(leafNodesAreConsistent(((PathIndexImpl) index).tree));
        }

        assert(!doEmptyChildrenExistInNode(((PathIndexImpl) index).tree.rootNodePageID, ((PathIndexImpl) index).tree));
    }

    public boolean doEmptyChildrenExistInNode(long nodeId, Tree tree) throws IOException {
        Node node = tree.getNode(nodeId);
        if(nodeId == tree.rootNodePageID && node.keys.size() == 0) {
            return false;
        }
        if(node.keys.size() == 0){
            return true;
        }
        if(node instanceof InternalNode){
            for(long childId : ((InternalNode)node).children){
                if(doEmptyChildrenExistInNode(childId, tree)){
                    return true;
                }
            }
        }
        System.out.println(nodeString(node, new StringBuilder()));
        return false;
    }
}
