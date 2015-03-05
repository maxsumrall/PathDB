package bptree;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.util.LinkedList;
import java.util.Random;

public class PathIndexTest extends TestCase {
    private PathIndex index;
    private File indexFile;


    @Before
    public void initializeIndex() throws FileAlreadyExistsException {
        indexFile = new File(PathIndex.DEFAULT_FILE_NAME);
        index = PathIndex.emptyPathIndex(indexFile)
                        .kValues(minK, maxK)
                        .buildLabelPathMapping(labelPaths)
                        .setSignatures(signatures);
        assertTrue(index.ready());
    }
    @After
    public void destroyIndex(){
        indexFile.delete();
    }

    @Test
    public void testFileAlreadyExistsException(){
        try {
            PathIndex indexB = PathIndex.emptyPathIndex(new File(PathIndex.DEFAULT_FILE_NAME));
            fail("indexB on already existing file did not cause exception");
        }
        catch(FileAlreadyExistsException ignored){}
    }

    @Test
    public void testInsertSequentialKeysIntoIndex(){
        int number_of_keys_to_insert = 1000;
        int k = 3;
        LinkedList<Long[]> keys = new LinkedList<>();
        for(int i = 0; i < number_of_keys_to_insert; i++){
            keys.add(KeyTest.generateSequentialKey(k));
            index.insert(keys.getLast());
        }
        Cursor cursor;
        for(Long[] key : keys){
            cursor = index.find(key);
            assert(cursor.hasNext());
            assert(cursor.next() == key);
        }
    }

    @Test
    public void testInsertRandomKeysIntoIndex(){
        int number_of_keys_to_insert = 1000;
        int k = 3;
        LinkedList<Long[]> keys = new LinkedList<>();
        for(int i = 0; i < number_of_keys_to_insert; i++){
            keys.add(KeyTest.generateRandomKey(k));
            index.insert(keys.getLast());
        }
        Cursor cursor;
        for(Long[] key : keys){
            cursor = index.find(key);
            assert(cursor.hasNext());
            assert(cursor.next() == key);
        }
    }

    @Test
    public void testInsertRandomKeysWithRandomLengthIntoIndex(){
        int number_of_keys_to_insert = 1000;
        Random random = new Random();
        LinkedList<Long[]> keys = new LinkedList<>();
        for(int i = 0; i < number_of_keys_to_insert; i++){
            keys.add(KeyTest.generateSequentialKey(random.nextInt(3) + 4));
            index.insert(keys.getLast());
        }
        Cursor cursor;
        for(Long[] key : keys){
            cursor = index.find(key);
            assert(cursor.hasNext());
            assert(cursor.next() == key);
        }
    }
}