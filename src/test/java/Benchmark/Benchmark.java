/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package Benchmark;

import PathIndex.PageProxyCursor;
import PathIndex.DiskCache;
import PathIndex.IndexTree;
import PathIndex.SearchCursor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.neo4j.io.pagecache.PagedFile;

public class Benchmark {

    public static Random random;

    public static IndexTree proxy;

    public static void main(String[] args) throws IOException {

        //System.out.println("------ 1000 -------");
        //runInsertionExperiment(1000);

        System.out.println("------ 10000 -------");
        runExperiment(10000);

        System.out.println("------ 100000 -------");
        runExperiment(100000);

        System.out.println("------ 1,000,000 -------");
        runExperiment(1000000);

        System.out.println("------ 10,000,000 -------");
        runExperiment(10000000);

        //System.out.println("------ 100,000,000 -------");
        //runExperiment(100000000);

       // System.out.println("------ 1,000,000,000 -------");
       // runExperiment(1000000000);

        System.out.println("Benchmarking completed.");
    }

    public static void runExperiment(int items_to_insert) throws IOException {
        DiskCache disk = DiskCache.temporaryDiskCache(items_to_insert + "experiment.dat", false);
        proxy = new IndexTree(4, disk);

        int number_of_paths = 10000;



        double totalSumInsert = performInsertionExperiment(proxy, items_to_insert, number_of_paths);
        double totalSumSearch = performSearchExperiment(proxy, items_to_insert, number_of_paths);
        double totalSumDelete = performDeletionExperiment(proxy, items_to_insert, number_of_paths);

        disk.shutdown();
        long disk_size = disk.pageCacheFile.length();

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("\n -------").append(items_to_insert).append("-------").append((new Date().toString()));
        //strBuilder.append("\n Depth of tree ").append(depth);
        strBuilder.append("\n Sum Insertion time(minutes): ").append(totalSumInsert / 60000000000d);
        strBuilder.append("\n Average Insertion time(micro seconds): ").append(totalSumInsert / items_to_insert);
        strBuilder.append("\n Average Search time(micro seconds): ").append(totalSumSearch / items_to_insert);
        strBuilder.append("\n Average Deletion time(micro seconds): ").append(totalSumDelete / items_to_insert);
        strBuilder.append("\n Disk Size(mb): ").append(disk_size / 1000000.0);

        logToFile(strBuilder.toString());
    }

    public static double performInsertionExperiment(IndexTree tree, int items_to_insert, int number_of_paths){
        double totalSum = 0;
        long[] key = new long[4];
        for (int i = 1; i < items_to_insert; i++) {
            key[0] = (long) (i);
            key[1] = (long) i;
            key[2] = (long) i;
            key[3] = (long) i;
            long startTime = System.nanoTime();
            //Do timed operation here

            tree.insert(key);

            long endTime = System.nanoTime();

            long duration = (endTime - startTime);
            totalSum += duration / 1000;//convert to from nanoseconds to microseconds.
        }
        return totalSum;
    }

    public static double performSearchExperiment(IndexTree tree, int items_to_insert, int number_of_paths) throws IOException {
        double totalSum = 0;
        long[] key = new long[4];
        for (int i = 1; i < items_to_insert; i++) {
            key[0] = (long) (i % number_of_paths);
            key[1] = (long) i;
            key[2] = (long) i;
            key[3] = (long) i;
            long startTime = System.nanoTime();
            //Do timed operation here

            SearchCursor result = tree.find(key);
            try(PageProxyCursor cursor = tree.disk.getCursor(
                    result.pageID, PagedFile.PF_SHARED_LOCK)) {
                assert Arrays.equals(key, result.next(cursor));
            } catch (IOException e) {
                e.printStackTrace();
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime);
            totalSum += duration / 1000;//convert to from nanoseconds to microseconds.
        }
        return totalSum;
    }

    public static double performDeletionExperiment(IndexTree tree, int items_to_insert, int number_of_paths){
        double totalSum = 0;
        long[] key = new long[4];
        for (int i = 1; i < items_to_insert; i++) {
            key[0] = (long) (i % number_of_paths);
            key[1] = (long) i;
            key[2] = (long) i;
            key[3] = (long) i;

            long startTime = System.nanoTime();
            //Do timed operation here

            tree.remove(key);
            //SearchCursor result = tree.find(key);

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
            System.out.println(text);
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }
}
