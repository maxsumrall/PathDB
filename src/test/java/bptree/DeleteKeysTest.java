package bptree;

import bptree.impl.PathIndexImpl;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static bptree.LabelsAndPathsGenerator.*;

/**
 * Created by max on 3/24/15.
 */
public class DeleteKeysTest {
    private Index index;
    private File indexFile;
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
    public void testInsertSequentialKeysIntoIndex() throws IOException {
        int number_of_keys_to_insert = 1000;
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
    }

    @Test
    public void testInsertRandomKeysIntoIndex() throws IOException {
        int number_of_keys_to_insert = 1000;
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
    }
}
