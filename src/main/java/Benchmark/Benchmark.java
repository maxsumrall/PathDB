package Benchmark;

import bptree.Index;
import bptree.impl.NodeInsertion;
import bptree.impl.NodeSearch;
import bptree.impl.NodeTree;
import bptree.impl.PathIndexImpl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;


public class Benchmark {

    public static Random random;
    public static Index index;
    public static LinkedList<Long[]> labelPaths;
    public static PathIndexImpl pindex;
    public static NodeTree proxy;

    public static void main(String[] args) throws IOException {

        //System.out.println("------ 1000 -------");
        //runInsertionExperiment(1000);

        //System.out.println("------ 10000 -------");
        //runInsertionExperiment(10000);

        //System.out.println("------ 100000 -------");
        //runExperiment(100000);

        System.out.println("------ 1000000 -------");
        runExperiment(1000000);

        //System.out.println("------ 10,000,000 -------");
        //runExperiment(10000000);

        //System.out.println("------ 100,000,000 -------");
        //runExperiment(100000000);

       // System.out.println("------ 1,000,000,000 -------");
       // runExperiment(1000000000);


        System.out.println("Benchmarking completed.");
    }

    public static void runExperiment(int items_to_insert) throws IOException {
        labelPaths = exampleLabelPaths(2, 2);
        index = PathIndexImpl.getTemporaryPathIndex()
                .setRangeOfPathLengths(2, 2)
                .setLabelPaths(labelPaths)
                .setSignaturesToDefault();

        pindex = ((PathIndexImpl) index);
        proxy = new NodeTree(pindex.tree.rootNodePageID, pindex.tree.nodeKeeper.diskCache.pagedFile);

        int number_of_paths = 10000;

        /*
        long[][] keys = new long[items_to_insert][4];
        for (int i = 0; i < keys.length; i++) {
            keys[i][0] = (long) (i % number_of_paths);
            keys[i][1] = (long) i;
            keys[i][2] = (long) i;
            keys[i][3] = (long) i;
        }
*/
        int disk_size = 0;

        double totalSumInsert = performInsertionExperiment(proxy, items_to_insert, number_of_paths);
        disk_size = index.indexSize();
        int depth = index.getDepthOfTree();
        double totalSumSearch = performSearchExperiment(proxy, items_to_insert, number_of_paths);
        //double totalSumDelete = performDeletionExperiment(proxy, keys, items_to_insert, number_of_paths);
        index.shutdown();


        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("\n -------").append(items_to_insert).append("-------").append((new Date().toString()));
        strBuilder.append("\n Depth of tree ").append(depth);
        strBuilder.append("\n Sum Insertion time(minutes): ").append(totalSumInsert / 60000000000d);
        strBuilder.append("\n Average Insertion time(micro seconds): ").append(totalSumInsert / items_to_insert);
        strBuilder.append("\n Average Search time(micro seconds): ").append(totalSumSearch / items_to_insert);
        //strBuilder.append("\n Average Deletion time(micro seconds): ").append(totalSumDelete / items_to_insert);
        strBuilder.append("\n Disk Size(mb): ").append(disk_size);

        logToFile(strBuilder.toString());
    }

    public static double performInsertionExperiment(NodeTree index, int items_to_insert, int number_of_paths){
        double totalSum = 0;
        long[] key = new long[4];
        for (int i = 0; i < items_to_insert; i++) {
            key[0] = (long) (i % number_of_paths);
            key[1] = (long) i;
            key[2] = (long) i;
            key[3] = (long) i;
            long startTime = System.nanoTime();
            //Do timed operation here

            NodeInsertion.insert(key);

            long endTime = System.nanoTime();

            long duration = (endTime - startTime);
            totalSum += duration / 1000;//convert to from nanoseconds to microseconds.
        }
        return totalSum;
    }

    public static double performSearchExperiment(NodeTree index, int items_to_insert, int number_of_paths){
        double totalSum = 0;
        long[] key = new long[4];
        for (int i = 0; i < items_to_insert; i++) {
            key[0] = (long) (i % number_of_paths);
            key[1] = (long) i;
            key[2] = (long) i;
            key[3] = (long) i;
            long startTime = System.nanoTime();
            //Do timed operation here

            NodeSearch.find(key);

            long endTime = System.nanoTime();

            long duration = (endTime - startTime);
            totalSum += duration / 1000;//convert to from nanoseconds to microseconds.
        }
        return totalSum;
    }

    public static double performDeletionExperiment(NodeTree index, int items_to_insert, int number_of_paths){
        double totalSum = 0;
        long[] key = new long[4];
        for (int i = 0; i < items_to_insert; i++) {
            key[0] = (long) (i % number_of_paths);
            key[1] = (long) i;
            key[2] = (long) i;
            key[3] = (long) i;

            long startTime = System.nanoTime();
            //Do timed operation here

            //index.remove(key);

            long endTime = System.nanoTime();

            long duration = (endTime - startTime);
            totalSum += duration / 1000;//convert to from nanoseconds to microseconds.
        }
        return totalSum;
    }

    public static Long calculateSum(List<Long> items){
        Long sum = 0l;
        for(Long num : items){
            sum = sum + num;
        }
        return sum;
    }

    public static Long calculateAverage(List<Long> items, Long sum){
        Long average = sum/items.size();
        return average;
    }


    public static LinkedList<Long[]> exampleLabelPaths(int number_of_paths, int k) {
        Random random = new Random();
        LinkedList<Long[]> labelPaths = new LinkedList<>();
        for (int i = 0; i < number_of_paths; i++) {
            labelPaths.add(new Long[k]);
            for (int j = 0; j < k; j++) {
                labelPaths.get(i)[j] = Math.abs(random.nextLong());
            }
        }
        return labelPaths;
    }

    public static void logToFile(String text){
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("benchmarking_results.txt", true)))) {
            out.println(text);
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }
}