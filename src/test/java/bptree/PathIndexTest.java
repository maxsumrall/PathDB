package bptree;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Random;

public class PathIndexTest extends TestCase {
    private PathIndex index;
    private File indexFile;
    private ArrayList<Long[]> labelPaths;

    /**
     * Generates Label Paths with random relationship ids.
     * @param number_of_paths The number of paths to generate.
     * @param k The length of paths to generate.
     * @return An ArrayList containing labeled paths each of k length.
     */
    public ArrayList<Long[]> exampleLabelPaths(int number_of_paths, int k){
        Random random = new Random();
        ArrayList<Long[]> labelPaths = new ArrayList<>(number_of_paths);
        for(Long[] path:labelPaths){
            for(int i = 0; i < k; i++){
                path[i] = random.nextLong();
            }
        }
        return labelPaths;
    }

    /**
     * Generates Label Paths with random relationship ids.
     * @param number_of_paths The number of paths to generate.
     * @param minK The minimum length of paths to generate.
     * @param maxK The maximum length of paths to generate.
     * @return An ArrayList containing labeled paths each between minK and maxK length.
     */
    public ArrayList<Long[]> exampleVariableLengthLabelPaths(int number_of_paths, int minK, int maxK){
        Random random = new Random();
        ArrayList<Long[]> labelPaths = new ArrayList<>();
        for(int i = 0; i < number_of_paths; i++){
            labelPaths.set(i, new Long[random.nextInt(maxK - minK + 1) + minK]);
            for(int j = 0; j < labelPaths.get(i).length; j++){
                labelPaths.get(i)[j] = random.nextLong();
            }
        }
        return labelPaths;
    }

    /**
     * Generate keys ready to be inserted into the path index. Node ID's set to random values.
     * @param labelPaths The label paths to use for generating the keys.
     * @param number_of_keys The number of keys to generate
     * @return An ArrayList of keys ready to be inserted into the database.
     */
    public ArrayList<Long[][]> exampleRandomKeys(ArrayList<Long[]> labelPaths, int number_of_keys) {
        return exampleKeys(labelPaths, number_of_keys, true);
    }
    /**
     * Generate keys ready to be inserted into the path index.
     * Node ID's set to sequential values from 0 to the parameter number_of_keys.
     * @param labelPaths The label paths to use for generating the keys.
     * @param number_of_keys The number of keys to generate
     * @return An ArrayList of keys ready to be inserted into the database.
     */
    public ArrayList<Long[][]> exampleSequentialKeys(ArrayList<Long[]> labelPaths, int number_of_keys) {
        return exampleKeys(labelPaths, number_of_keys, false);
    }

    private ArrayList<Long[][]> exampleKeys(ArrayList<Long[]> labelPaths, int number_of_keys, boolean randomNodeIds) {
        Random random = new Random();
        ArrayList<Long[][]> keys = new ArrayList<>(number_of_keys);
        for (int i = 0; i < keys.size(); i++) {
            Long[] randomPath = labelPaths.get(random.nextInt(labelPaths.size()));
            Long[] nodes = new Long[randomPath.length + 1];
            for (int j = 0; j < nodes.length; j++) {
                nodes[i] = randomNodeIds ? random.nextLong() : i;
                keys.set(i, new Long[][]{randomPath, nodes});
            }
        }
        return keys;
    }




    @Before
    public void initializeIndex() throws FileAlreadyExistsException {
        labelPaths = exampleLabelPaths(20,2);
        index = PathIndex.temporaryPathIndex()
                        .setKValues(2, 2)
                        .buildLabelPathMapping(labelPaths)
                        .setSignatures(PathIndex.defaultSignatures(2,2));
        assertTrue(index.ready());
    }

    @Test
    public void testInsertSequentialKeysIntoIndex(){
        int number_of_keys_to_insert = 1000;
        ArrayList<Long[][]> keys = exampleSequentialKeys(labelPaths, number_of_keys_to_insert);
        for(Long[][] key: keys){
            index.insert(key[0], key[1]);
        }
        Cursor cursor;
        for(Long[][] key : keys){
            cursor = index.find(key[0], key[1]);
            assert(cursor.hasNext());
            assert(cursor.next().equals(new Long[][]{new Long[]{}})); //the empty set
        }
    }

    @Test
    public void testInsertRandomKeysIntoIndex(){
        int number_of_keys_to_insert = 1000;
        ArrayList<Long[][]> keys = exampleRandomKeys(labelPaths, number_of_keys_to_insert);
        for(Long[][] key: keys){
            index.insert(key[0], key[1]);
        }
        Cursor cursor;
        for(Long[][] key : keys){
            cursor = index.find(key[0], key[1]);
            assert(cursor.hasNext());
            assert(cursor.next().equals(new Long[][]{new Long[]{}})); //the empty set
        }
    }
    @Test
    public void testInsertRandomKeysWithRandomLengthIntoIndex(){
        int number_of_keys_to_insert = 1000;
        ArrayList<Long[]> different_length_paths  = exampleVariableLengthLabelPaths(number_of_keys_to_insert, 2, 4);
        ArrayList<Long[][]> keys = exampleRandomKeys(different_length_paths, number_of_keys_to_insert);
        for(Long[][] key: keys){
            index.insert(key[0], key[1]);
        }
        Cursor cursor;
        for(Long[][] key : keys){
            cursor = index.find(key[0], key[1]);
            assert(cursor.hasNext());
            assert(cursor.next().equals(new Long[][]{new Long[]{}})); //the empty set
        }
    }
}