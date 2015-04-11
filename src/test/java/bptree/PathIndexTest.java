package bptree;

import bptree.impl.KeyImpl;
import bptree.impl.Node;
import bptree.impl.PathIndexImpl;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

import static bptree.LabelsAndPathsGenerator.*;

public class PathIndexTest {
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
    @Test
    public void testInsertRandomKeysWithRandomLengthIntoIndex() throws IOException {
        int number_of_keys_to_insert = 1000;
        ArrayList<Long[]> different_length_paths  = exampleVariableLengthLabelPaths(number_of_keys_to_insert, 2, 4);
        ArrayList<Key> keys = exampleRandomKeys(different_length_paths, number_of_keys_to_insert);
        index = PathIndexImpl.getTemporaryPathIndex()
                .setRangeOfPathLengths(2, 4)
                .setLabelPaths(different_length_paths)
                .setSignaturesToDefault();
        for(Key key: keys){
            index.insert(key);
        }
        Cursor cursor;
        for(Key key : keys){
            cursor = index.find(key);
            assert(cursor.hasNext());
            assert(Arrays.equals(cursor.next(), index.buildComposedKey(key)));
        }
    }

    @Test
    public void testPrefixCheckingMultipleResults() throws IOException {
        int number_of_keys_to_insert = 2000;
        ArrayList<Long[]> different_length_paths = exampleVariableLengthRandomLabelPaths(50, 2, 4);
        ArrayList<Key> keys = exampleRandomKeys(different_length_paths, number_of_keys_to_insert);
        index = PathIndexImpl.getTemporaryPathIndex()
                .setRangeOfPathLengths(2, 4)
                .setLabelPaths(different_length_paths)
                .setSignaturesToDefault();
        LinkedList<Long[]> keys_built = new LinkedList<>();
        for (Key key : keys) {
            keys_built.add(index.buildComposedKey(key));
        }
        for (Key key : keys) {
            index.insert(key);
        }
        Cursor cursor;
        HashMap<Long[], Integer> results = new HashMap<>();
        for (Long[] path: different_length_paths) {
            cursor = index.find(new KeyImpl(path, new Long[]{}));
            while (cursor.hasNext()) {
                Long[] result = cursor.next();
                if(!Node.keyComparator.validPrefix(index.buildComposedKey(new KeyImpl(path, new Long[]{})), result)){
                    break;
                }
                assert(Node.keyComparator.validPrefix(index.buildComposedKey(new KeyImpl(path, new Long[]{})), result));
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
        ArrayList<Key> keys = exampleRandomKeys(different_length_paths, number_of_keys_to_insert);
        index = PathIndexImpl.getTemporaryPathIndex()
                .setRangeOfPathLengths(2, 4)
                .setLabelPaths(different_length_paths)
                .setSignaturesToDefault();
        LinkedList<Long[]> keys_built = new LinkedList<>();
        for (Key key : keys) {
            keys_built.add(index.buildComposedKey(key));
        }
        for (Key key : keys) {
            index.insert(key);
        }
        Cursor cursor;
        HashMap<Long[], Integer> results = new HashMap<>();
        for (int i = 0; i < keys.size(); i++){
            Key key = keys.get(i);

            if(i%3 == 0) {
                cursor = index.find(new KeyImpl(key.getLabelPath(), new Long[]{}));
            }
            else if (i%3 == 1){
                cursor = index.find(new KeyImpl(key.getLabelPath(), new Long[]{key.getNodes()[0]}));

            }
            else{
                cursor = index.find(new KeyImpl(key.getLabelPath(), new Long[]{key.getNodes()[1]}));
            }
            int resultsFound = 0;
            int cursorSize = cursor.size();
            while (cursor.hasNext()) {
                resultsFound++;
                Long[] result = cursor.next();
                assert(Node.keyComparator.validPrefix(index.buildComposedKey(new KeyImpl(key.getLabelPath(), new Long[]{})), result));
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
        printTree(((PathIndexImpl)index).tree.getNode(((PathIndexImpl)index).tree.rootNodePageID), ((PathIndexImpl)index).tree);


    }




}