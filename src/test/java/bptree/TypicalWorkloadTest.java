package bptree;

import bptree.impl.KeyImpl;
import bptree.impl.Node;
import bptree.impl.PathIndexImpl;
import bptree.impl.Tree;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static bptree.LabelsAndPathsGenerator.exampleLabelPaths;
import static bptree.LabelsAndPathsGenerator.exampleSequentialKeys;

/**
 * Created by max on 4/21/15.
 */
public class TypicalWorkloadTest {
    private Index index;
    private Tree tree;
    private File indexFile;
    private ArrayList<Long[]> labelPaths;


    private void indexIsConsistentAssertions(Index index, List<Key> keysThatShouldBeInIndex, List<Key> keysThatShouldNotBeInIndex){
        assert(tree.getKeySetSize() == tree.getKeySetSizeForceCheck());
        assert(tree.getKeySetSize() == keysThatShouldBeInIndex.size());
        ableToFindAllKeys(index, keysThatShouldBeInIndex, keysThatShouldNotBeInIndex);
    }

    private void ableToFindAllKeys(Index index, List<Key> keysThatShouldBeInIndex, List<Key> keysThatShouldNotBeInIndex){
        for(int i = 0; i < keysThatShouldBeInIndex.size(); i++){
            Cursor cursor = index.find(keysThatShouldBeInIndex.get(i));
            assert(cursor.size() == 1);
        }
        for(int i = 0; i < keysThatShouldNotBeInIndex.size(); i++) {
            Cursor cursor = index.find(keysThatShouldNotBeInIndex.get(i));
            assert (cursor.size() == 0);
        }
    }

    @Before
    public void initializeIndex() throws IOException {
        labelPaths = exampleLabelPaths(20, 2);
        index = PathIndexImpl.getTemporaryPathIndex()
                .setRangeOfPathLengths(2, 2)
                .setLabelPaths(labelPaths)
                .setSignaturesToDefault();
        tree = (( PathIndexImpl)index).tree;
    }

    @Test
    public void typicalWorkload(){
        int number_of_keys_to_insert = 10000;
        ArrayList<Key> keysMain = exampleSequentialKeys(labelPaths, number_of_keys_to_insert);
        ArrayList<Key> keys = new ArrayList<>(keysMain.subList(0,5000));
        LinkedList<Key> keysInIndex = new LinkedList<>();
        LinkedList<Key> keysDeletedFromIndex = new LinkedList<>();
        //do inserts, deletions, make sure index is consistent with these two above lists.

        //Insert
        for(Key key : keys){
            keysInIndex.add(key);
            index.insert(key);
        }

        indexIsConsistentAssertions(index, keysInIndex, keysDeletedFromIndex);


        //Delete
        int index_to_start_from = 2000;
        int how_many_to_delete = 1000;
        Key delete_key;
        for(int i = index_to_start_from; i < how_many_to_delete + index_to_start_from; i++){
            delete_key = keysInIndex.remove(i);
            keysDeletedFromIndex.add(delete_key);
            index.remove(delete_key);
        }

        indexIsConsistentAssertions(index, keysInIndex, keysDeletedFromIndex);


        //Insert
        ArrayList<Key> newKeys = new ArrayList<>(keysMain.subList(5000, keysMain.size()));
        for(Key key: newKeys){
            keysInIndex.add(key);
            index.insert(key);
        }

        indexIsConsistentAssertions(index, keysInIndex, keysDeletedFromIndex);

        //Delete

        delete_key = keysInIndex.getFirst();
        Key broadKey = new KeyImpl(delete_key.getLabelPath(), new Long[]{});
        //int removedKeys = index.remove(broadKey).getN();
        Long[] broadComposedKey = index.buildComposedKey(broadKey);
        LinkedList<Key> indexesToRemove = new LinkedList<>();
        for(int i = 0; i < keysInIndex.size(); i++){
            Key currKey = keysInIndex.get(i);
            if( Node.keyComparator.validPrefix(broadComposedKey, index.buildComposedKey(currKey))){
                indexesToRemove.add(currKey);
                index.remove(currKey);
            }
        }
        for(Key key : indexesToRemove){
            keysInIndex.remove(key);
            keysDeletedFromIndex.add(key);
        }

        indexIsConsistentAssertions(index, keysInIndex, keysDeletedFromIndex);

    }
}
