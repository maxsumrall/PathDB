package bptree;

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
    private PathIndex index;
    private File indexFile;
    private ArrayList<Long[]> labelPaths;

    @Before
    public void initializeIndex() throws IOException {
        labelPaths = exampleLabelPaths(20,2);
        index = PathIndex.temporaryPathIndex()
                .setKValues(2, 2)
                .buildLabelPathMapping(labelPaths)
                .setSignatures(PathIndex.defaultSignatures(2,2));
        assert(index.ready());
    }

    @Test
    public void testInsertSequentialKeysIntoIndex() throws IOException {
        int number_of_keys_to_insert = 1000;
        ArrayList<Long[][]> keys = exampleSequentialKeys(labelPaths, number_of_keys_to_insert);
        for(Long[][] key: keys){
            index.insert(key[0], key[1]);
        }
        Cursor cursor;
        for(Long[][] key : keys){
            cursor = index.find(key[0], key[1]);
            assert(cursor.hasNext());
            assert(Arrays.equals(cursor.next(), index.build_searchKey(key[0], key[1]))); //the empty set
        }
    }

    @Test
    public void testInsertRandomKeysIntoIndex() throws IOException {
        int number_of_keys_to_insert = 1000;
        ArrayList<Long[][]> keys = exampleRandomKeys(labelPaths, number_of_keys_to_insert);
        for(Long[][] key: keys){
            index.insert(key[0], key[1]);
        }
        Cursor cursor;
        for(Long[][] key : keys){
            cursor = index.find(key[0], key[1]);
            assert(cursor.hasNext());
            assert(Arrays.equals(cursor.next(), index.build_searchKey(key[0], key[1]))); //the empty set
        }
    }
}
