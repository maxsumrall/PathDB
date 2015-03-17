package bptree;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class PathIndexTest {
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
        ArrayList<Long[]> labelPaths = new ArrayList<>();
        for(int i = 0; i < number_of_paths; i++){
            labelPaths.add(new Long[k]);
            for(int j = 0; j < k; j++){
                labelPaths.get(i)[j] = Math.abs(random.nextLong());
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
            labelPaths.add(new Long[random.nextInt(maxK - minK + 1) + minK]);
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
        ArrayList<Long[][]> keys = new ArrayList<>();
        for (int i = 0; i < number_of_keys; i++) {
            Long[] randomPath = labelPaths.get(random.nextInt(labelPaths.size()));
            Long[] nodes = new Long[randomPath.length + 1];
            for (int j = 0; j < nodes.length; j++) {
                nodes[j] = randomNodeIds ? random.nextLong() : i;
            }
            keys.add(new Long[][]{randomPath, nodes});
        }
        return keys;
    }

    public static void printTree(Node node, Tree tree) throws IOException {
        printNode(node);
        if(node instanceof InternalNode){
            for(Long child : ((InternalNode)node).children){
                printTree(tree.getNode(child), tree);
            }
        }

    }
    public static void printNode(Node node){
        System.out.println((node instanceof LeafNode ? "Leaf Node, " : "Internal Node, ") + "Node ID: " + node.id);
        System.out.print("Keys: ");
        for(Long[] key : node.keys){
            System.out.print(Arrays.toString(key) + ", ");
        }
        System.out.print("\n");
        if(node instanceof InternalNode){
            System.out.print("Children: ");
            for(Long child : ((InternalNode)node).children){
                System.out.print(child + ", ");
            }
            System.out.print("\n");
        }
    }

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
    @Test
    public void testInsertRandomKeysWithRandomLengthIntoIndex() throws IOException {
        int number_of_keys_to_insert = 1000;
        ArrayList<Long[]> different_length_paths  = exampleVariableLengthLabelPaths(number_of_keys_to_insert, 2, 4);
        ArrayList<Long[][]> keys = exampleRandomKeys(different_length_paths, number_of_keys_to_insert);
        index = PathIndex.temporaryPathIndex()
                .setKValues(2, 4)
                .buildLabelPathMapping(different_length_paths)
                .setSignatures(PathIndex.defaultSignatures(2,4));
        for(Long[][] key: keys){
            index.insert(key[0], key[1]);
        }
        Cursor cursor;
        for(Long[][] key : keys){
            cursor = index.find(key[0], key[1]);
           /* if(!Arrays.equals(cursor.next(), index.build_searchKey(key[0], key[1]))){
                Tree tree  = index.tree;
                System.out.println("Search Key: " + Arrays.toString(index.build_searchKey(key[0], key[1])));
                System.out.println(tree.logger);
                printTree(tree.getNode(tree.rootNodePageID), tree);
                cursor = index.find(key[0], key[1]);

            }*/
            assert(cursor.hasNext());
            assert(Arrays.equals(cursor.next(), index.build_searchKey(key[0], key[1])));
        }
    }


    @Test
    public void testPrefixCheckingMultipleResults() throws IOException {
        int number_of_keys_to_insert = 1000;
        ArrayList<Long[]> different_length_paths = exampleVariableLengthLabelPaths(number_of_keys_to_insert, 2, 4);
        ArrayList<Long[][]> keys = exampleRandomKeys(different_length_paths, number_of_keys_to_insert);
        index = PathIndex.temporaryPathIndex()
                .setKValues(2, 4)
                .buildLabelPathMapping(different_length_paths)
                .setSignatures(PathIndex.defaultSignatures(2, 4));
        LinkedList<Long[]> keys_built = new LinkedList<>();
        for (Long[][] key : keys) {
            keys_built.add(index.build_searchKey(key[0], key[1]));
        }
        for (Long[][] key : keys) {
            index.insert(key[0], key[1]);
        }
        Cursor cursor;
        LinkedList<Long[]> results = new LinkedList<>();
        for (Long[][] key : keys) {
            cursor = index.find(key[0], new Long[]{});
            while (cursor.hasNext()) {
                Long[] result = cursor.next();
                results.add(result);
            }
        }
        System.out.println(keys_built.size() + " " + results.size());
        //assert (keys_built.size() == results.size());



    }



}