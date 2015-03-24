package bptree;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static bptree.LabelsAndPathsGenerator.*;

public class PathIndexTest {
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
           // if(!Arrays.equals(cursor.next(), index.build_searchKey(key[0], key[1]))){
            //    Tree tree  = index.tree;
            //    System.out.println("Search Key: " + Arrays.toString(index.build_searchKey(key[0], key[1])));
            //    System.out.println(tree.logger);
            //    printTree(tree.getNode(tree.rootNodePageID), tree);
            //    cursor = index.find(key[0], key[1]);
           // }
            assert(cursor.hasNext());
            assert(Arrays.equals(cursor.next(), index.build_searchKey(key[0], key[1])));
        }
    }


    @Test
    public void testPrefixCheckingMultipleResults() throws IOException {
        int number_of_keys_to_insert = 2000;
        ArrayList<Long[]> different_length_paths = exampleVariableLengthRandomLabelPaths(50, 2, 4);
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
        HashMap<Long[], Integer> results = new HashMap<>();
        for (Long[] path: different_length_paths) {
            cursor = index.find(path, new Long[]{});
            while (cursor.hasNext()) {
                Long[] result = cursor.next();
                if(!Node.keyComparator.validPrefix(index.build_searchKey(path, new Long[]{}), result)){
                    break;
                }
                assert(Node.keyComparator.validPrefix(index.build_searchKey(path, new Long[]{}), result));
                results.put(result, 1);
            }
        }
        System.out.println(keys_built.size() + " " + results.size());

        for(Long[] result : results.keySet()){
            ArrayList<Long[]> intermediate = new ArrayList<>();
            for(Long[] saved : keys_built){
                if(Arrays.equals(saved, result)){
                    intermediate.add(saved);
                }
            }
            for(Long[] each : intermediate) {
                keys_built.remove(each);
            }
            intermediate.clear();

        }
        for(Long[] lost : keys_built){
            System.out.println(Arrays.toString(lost));
        }


    }


    @Test
    public void testVariablePrefixCheckingMultipleResults() throws IOException {
        int number_of_keys_to_insert = 500;
        ArrayList<Long[]> different_length_paths = exampleVariableLengthRandomLabelPaths(50, 2, 4);
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
        HashMap<Long[], Integer> results = new HashMap<>();
        for (int i = 0; i < keys.size(); i++){
            Long[][] key = keys.get(i);

            if(i%3 == 0) {
                cursor = index.find(key[0], new Long[]{});
            }
            else if (i%3 == 1){
                cursor = index.find(key[0], new Long[]{key[1][0]});
            }
            else{
                cursor = index.find(key[0], new Long[]{key[1][0], key[1][1]});
            }
            int resultsFound = 0;
            int cursorSize = cursor.size();
            while (cursor.hasNext()) {
                resultsFound++;
                Long[] result = cursor.next();
                assert(Node.keyComparator.validPrefix(index.build_searchKey(key[0], new Long[]{}), result));
                results.put(result, 1);
            }
            System.out.println("Found by .next(): " + resultsFound + " ||| cursor.size(): " + cursorSize);
            assert(resultsFound == cursorSize);
        }
        System.out.println(keys_built.size() + " " + results.size());

        for(Long[] result : results.keySet()){
            ArrayList<Long[]> intermediate = new ArrayList<>();
            for(Long[] saved : keys_built){
                if(Arrays.equals(saved, result)){
                    intermediate.add(saved);
                }
            }
            for(Long[] each : intermediate) {
                keys_built.remove(each);
            }
            intermediate.clear();

        }
        for(Long[] lost : keys_built){
            System.out.println(Arrays.toString(lost));
        }
        printTree(index.tree.getNode(index.tree.rootNodePageID), index.tree);


    }




}