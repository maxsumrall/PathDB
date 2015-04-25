package Benchmark;

import bptree.Index;
import bptree.Key;
import bptree.impl.PathIndexImpl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;


public class Benchmark {

    Random random;


    public static void main(String[] args) throws IOException {
        System.out.println("------ 1000 -------");
        runInsertionExperiment(1000);

        System.out.println("------ 10000 -------");
        runInsertionExperiment(10000);

        System.out.println("------ 100000 -------");
        runInsertionExperiment(100000);

        System.out.println("------ 1000000 -------");
        runInsertionExperiment(1000000);

       /* System.out.println("------ 10,000,000 -------");
        runInsertionExperiment(10000000);

        System.out.println("------ 100,000,000 -------");
        runInsertionExperiment(100000000);

        System.out.println("------ 1,000,000,000 -------");
        runInsertionExperiment(1000000000);
*/

        System.out.println("Benchmarking completed.");
    }

    public static void runInsertionExperiment(int items_to_insert) throws IOException {
        LinkedList<Long[]> labelPaths = exampleLabelPaths(10000, 2);
        Index index = PathIndexImpl.getTemporaryPathIndex()
                .setRangeOfPathLengths(2, 2)
                .setLabelPaths(labelPaths)
                .setSignaturesToDefault();
        //LinkedList<Long> durations = new LinkedList<>();
        Double totalSum = (double) 0;

        for(int i = 0; i < items_to_insert; i++){
            Long[] relationships = labelPaths.get(i%labelPaths.size());
            Long[] nodes = new Long[]{(long) i, (long) i, (long) i};
            Key key = index.buildKey(relationships, nodes);

            long startTime = System.nanoTime();
            //Do timed operation here

                index.insert(key);

           //     System.out.println(i);

            //
            long endTime = System.nanoTime();

            long duration = (endTime - startTime);
            //durations.add(duration);
            totalSum += duration / 1000 ;//convert to from nanoseconds to microseconds.
        }
        index.shutdown();

        //Long sum = calculateSum(durations);
        //Collections.sort(durations);

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("\n -------").append(items_to_insert).append("-------").append((new Date().toString()));
        strBuilder.append("\n Sum Insertion time(minutes): ").append(totalSum/60000000000d);
        strBuilder.append("\n Average Insertion time(micro seconds): ").append(totalSum/items_to_insert);
        //strBuilder.append("\n Quickest time: ").append(durations.getFirst());
        //strBuilder.append("\n Slowest time: ").append(durations.getLast());
        logToFile(strBuilder.toString());
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