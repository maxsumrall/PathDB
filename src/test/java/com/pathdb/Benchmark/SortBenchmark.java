/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.Benchmark;

import com.pathdb.pageCacheSort.SetIterator;
import com.pathdb.pageCacheSort.Sorter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class SortBenchmark {
    Sorter sorter;
    public static void main(String[] args) throws IOException {
        SortBenchmark sortBenchmark = new SortBenchmark();

        sortBenchmark.smallestExample();

        //sortBenchmark.randomSorting();
    }

    public SortBenchmark() throws IOException {

    }

    public void smallestExample() throws IOException {
        sorter = new Sorter(4);
        writeUnsortedKeysToSorter(sorter, 10000000);
        long startTime = System.nanoTime();

        SetIterator itr = sorter.sort();

        String text = "SmallExample - Merge Set Size:" + sorter.FAN_IN + " Duration: " + ((System.nanoTime() - startTime) / 1000000);
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("sorting_benchmarking.txt", true)))) {
            out.println(text);
            System.out.println(text);
        }
    }

    public void randomSorting() throws IOException {
        sorter = new Sorter(4);
        writeRandomKeysToSorter(sorter, 10000000);
        long startTime = System.nanoTime();

        SetIterator itr = sorter.sort();

        String text = "RandomExample - Merge Set Size:" + sorter.FAN_IN + " Duration: " + ((System.nanoTime() - startTime) / 1000000);
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("sorting_benchmarking.txt", true)))) {
            out.println(text);
            System.out.println(text);
        }
    }

    public void writeUnsortedKeysToSorter(Sorter sorter, int count) throws IOException {
        long[] key = new long[4];
        for(int i = 2; i < count; i++){
            if(i % 2 == 0){
                for(int j = 0; j < key.length; j++){
                    key[j] = (long) (i + 1);
                }
            }
            else{
                for(int j = 0; j < key.length; j++){
                    key[j] = (long)(i - 1);
                }
            }
            sorter.addUnsortedKey(key);
        }
    }

    public void writeRandomKeysToSorter(Sorter sorter, int count) throws IOException {
        long[] key = new long[4];
        Random random = new Random();
        for(int i = 2; i < count; i++){
            long rnd = Math.abs(random.nextLong());
            for(int j = 0; j < key.length; j++){
                key[j] = rnd;
            }
            sorter.addUnsortedKey(key);
        }
    }


}
