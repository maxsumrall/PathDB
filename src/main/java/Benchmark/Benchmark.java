package Benchmark;

import bptree.Index;
import bptree.Key;
import bptree.impl.PathIndexImpl;

import java.io.IOException;
import java.util.*;


public class Benchmark {

    Random random;


    public static void main(String[] args) throws IOException {
       /* System.out.println("------ 1000 -------");
        runInsertionExperiment(1000);

        System.out.println("------ 10000 -------");
        runInsertionExperiment(10000);

        System.out.println("------ 100000 -------");
        runInsertionExperiment(100000);
*/
        System.out.println("------ 1000000 -------");
        runInsertionExperiment(1000000);
    }

    public static void runInsertionExperiment(int items_to_insert) throws IOException {
        LinkedList<Long[]> labelPaths = exampleLabelPaths(10000, 2);
        Index index = PathIndexImpl.getTemporaryPathIndex()
                .setRangeOfPathLengths(2, 2)
                .setLabelPaths(labelPaths)
                .setSignaturesToDefault();
        LinkedList<Long> durations = new LinkedList<>();


        for(int i = 0; i < items_to_insert; i++){
            Long[] relationships = labelPaths.get(i%labelPaths.size());
            Long[] nodes = new Long[]{new Long(i), new Long(i), new Long(i)};
            Key key = index.buildKey(relationships, nodes);

            long startTime = System.nanoTime();
            //Do timed operation here
            try {
                index.insert(key);
            }
            catch (Exception e){
                System.out.println(i);
                System.exit(1);
            }

            //
            long endTime = System.nanoTime();

            long duration = (endTime - startTime);
            durations.add(duration);
        }

        Long sum = calculateSum(durations);
        System.out.println("Sum Insertion time: " + sum);
        System.out.println("Average Insertion time: " + calculateAverage(durations, sum));
        Collections.sort(durations);
        System.out.println("Quickest time: " + durations.getFirst());
        System.out.println("Slowest time: " + durations.getLast());
        index.shutdown();
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
}
