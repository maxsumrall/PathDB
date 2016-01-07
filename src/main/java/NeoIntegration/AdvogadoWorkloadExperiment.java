/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

import NeoIntegration.AdvogadoBenchmarkExperiment;
import NeoIntegration.PathIDBuilder;
import PageCacheSort.Sorter;
import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.IndexBulkLoader;
import bptree.impl.IndexTree;
import bptree.impl.SearchCursor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.tooling.GlobalGraphOperations;

public class AdvogadoWorkloadExperiment {
    public HashMap<Integer, IndexTree> indexes = new HashMap<>();
    public HashMap<Integer, DiskCache> disks = new HashMap<>();
    public GraphDatabaseService database;
    public GlobalGraphOperations ggo;
    StringBuilder stringBuilder;
    HashMap<Long, PathIDBuilder> relationshipMap = new HashMap<>(); //relationship types to path ids
    HashMap<long[], Long> k2PathIds = new HashMap<>();
    HashMap<long[], Long> k3PathIds = new HashMap<>();
    long k2PathCounter = 1;
    long k3PathCounter = 1;
    String cypher;
    Sorter k1Sorter = new Sorter(3);
    long duration = 0;

    public static void main(String[] args) throws IOException {
        AdvogadoWorkloadExperiment workload = new AdvogadoWorkloadExperiment();
        workload.run();
        workload.disks.get(1).shutdown();
        workload.disks.get(2).shutdown();
        workload.disks.get(3).shutdown();

        System.out.println("Workload Index Size: " + (workload.disks.get(1).pageCacheFile.length() + workload.disks.get(2).pageCacheFile.length() + workload.disks.get(3).pageCacheFile.length()));

    }

    public AdvogadoWorkloadExperiment() throws IOException {

        DiskCache diskK2 = DiskCache.persistentDiskCache("AdvogadoworkloadK2.db", false);
        DiskCache diskK3 = DiskCache.persistentDiskCache("AdvogadoworkloadK3.db", false);

        disks.put(2, diskK2);
        disks.put(3, diskK3);

        IndexTree indexK2 = new IndexTree(4, diskK2);
        IndexTree indexK3 = new IndexTree(5, diskK3);
        indexes.put(2, indexK2);
        indexes.put(3, indexK3);



        stringBuilder = new StringBuilder();

        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(AdvogadoBenchmarkExperiment.DB_PATH).newGraphDatabase();

        ggo = GlobalGraphOperations.at(database);
    }


    public void run() throws IOException {
        //Start query 1

        long startTime = System.nanoTime();

        loadK1Index();

        query1();

        query2();

        query3();

        query4();

        query5();

        System.out.println("Total Query Workload Time: " + nanoToMilli(System.nanoTime() - startTime));


    }

    public void query1() throws IOException {
        long pathK1 = 560747377; // apprentice - >
        long pathK21 = 560747377; // apprentice - >
        long pathK22 = 560747377; //apprentice - >

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK21, pathK22);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 1-1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 1-1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 1-1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 1-1: Subsequent Search: " + subsequentSearch);

        duration = 0;


        starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2Version3(pathK1, pathID3);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 1-2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 1-2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 1-2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 1-2: Subsequent Search: " + subsequentSearch);
    }

    public void query2() throws IOException {
        long pathK1 = 1693961325; // journeyer - >
        long pathK21 = 1693961325; // journeyer - >
        long pathK22 = 1693961325; //journeyer - >

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK21, pathK22);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 2-1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 2-1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 2-1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 2-1: Subsequent Search: " + subsequentSearch);

        duration = 0;


        starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2Version3(pathK1, pathID3);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 2-2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 2-2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 2-2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 2-2: Subsequent Search: " + subsequentSearch);
    }


    public void query3() throws IOException {
        long pathK1 = 1081267614; // master - >
        long pathK21 = 1081267614; // master - >
        long pathK22 = 1081267614; //master - >

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK21, pathK22);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 3-1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 3-1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 3-1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 3-1: Subsequent Search: " + subsequentSearch);

        duration = 0;


        starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2Version3(pathK1, pathID3);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 3-2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 3-2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 3-2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 3-2: Subsequent Search: " + subsequentSearch);
    }


    public void query4() throws IOException {
        long pathK1 = 560747377; // apprentice - >
        long pathK21 = 1693961325; // journeyer - >
        long pathK22 = 1081267614; //master - >

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK21, pathK22);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 4-1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 4-1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 4-1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 4-1: Subsequent Search: " + subsequentSearch);

        duration = 0;


        starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2Version3(pathK1, pathID3);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 4-2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 4-2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 4-2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 4-2: Subsequent Search: " + subsequentSearch);
    }



    public void query5() throws IOException {
        long pathK1 = 560747377; // apprentice - >
        long pathK21 = 560747377; // apprentice - >
        long pathK22 = 1081267614; //master - >

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK21, pathK22);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 5-1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 5-1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 5-1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 5-1: Subsequent Search: " + subsequentSearch);

        duration = 0;


        starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2Version3(pathK1, pathID3);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 5-2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 5-2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 5-2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 5-2: Subsequent Search: " + subsequentSearch);
    }



    public long joinOnK1AndK2Version1(long pathK1, long pathK2) throws IOException {
        long[] resultB;
        long clock;
        int count = 0;
        List<long[]> entries = new ArrayList<>();
        long[] key = new long[]{pathK1, pathK2};
        if(!k3PathIds.containsKey(key))
            k3PathIds.put(key, k3PathCounter++);
        long pathID3 = k3PathIds.get(key);
        try (PageProxyCursor cursorK1 = disks.get(1).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
            try (PageProxyCursor cursorK2 = disks.get(2).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
                SearchCursor searchCursorA = indexes.get(1).find(cursorK1, new long[]{pathK1});
                while (searchCursorA.hasNext(cursorK1)) {
                    entries.add(searchCursorA.next(cursorK1));
                }
                for (long[] resultA : entries) {
                    SearchCursor searchCursorB = indexes.get(2).find(cursorK2, new long[]{pathK2, resultA[1]});
                    while (searchCursorB.hasNext(cursorK2)) {
                        resultB = searchCursorB.next(cursorK2);
                        if (resultA[2] == resultB[3]) {
                            count++;

                            //pause clock
                            clock = System.nanoTime();
                            indexes.get(3).insert(new long[]{pathID3, resultA[1], resultA[2], resultB[2], resultB[1]});
                            duration += System.nanoTime() - clock;
                            //start clock again
                        }
                    }
                }
            }
        }
        System.out.println("k3 joins found: " + count);
        return pathID3;
    }
    public long joinOnK1AndK2Version2(long pathK1, long pathK2) throws IOException {
        long[] resultB;
        long clock;
        int count = 0;
        List<long[]> entries = new ArrayList<>();
        long[] key = new long[]{pathK1, pathK2};
        if(!k3PathIds.containsKey(key))
            k3PathIds.put(key, k3PathCounter++);
        long pathID3 = k3PathIds.get(key);
        try (PageProxyCursor cursorK1 = disks.get(1).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
            try (PageProxyCursor cursorK2 = disks.get(2).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
                SearchCursor searchCursorA = indexes.get(1).find(cursorK1, new long[]{pathK1});
                while (searchCursorA.hasNext(cursorK1)) {
                    entries.add(searchCursorA.next(cursorK1));
                }
                for (long[] resultA : entries) {
                    SearchCursor searchCursorB = indexes.get(2).find(cursorK2, new long[]{pathK2, resultA[2]});
                    while (searchCursorB.hasNext(cursorK2)) {
                        resultB = searchCursorB.next(cursorK2);

                        count++;

                        //pause clock
                        clock = System.nanoTime();
                        indexes.get(3).insert(new long[]{pathID3, resultA[1], resultA[2], resultB[2], resultB[3]});
                        duration += System.nanoTime() - clock;
                        //start clock again

                    }
                }
            }
        }
        System.out.println("k3 joins found: " + count);
        return pathID3;
    }
    public long joinOnK1AndK2Version3(long pathK1, long pathK2) throws IOException {
        long[] resultB;
        long clock;
        int count = 0;
        List<long[]> entries = new ArrayList<>();
        long[] key = new long[]{pathK1, pathK2};
        if(!k3PathIds.containsKey(key))
            k3PathIds.put(key, k3PathCounter++);
        long pathID3 = k3PathIds.get(key);
        try (PageProxyCursor cursorK1 = disks.get(1).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
            try (PageProxyCursor cursorK2 = disks.get(2).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
                SearchCursor searchCursorA = indexes.get(1).find(cursorK1, new long[]{pathK1});
                while (searchCursorA.hasNext(cursorK1)) {
                    entries.add(searchCursorA.next(cursorK1));
                }
                for (long[] resultA : entries) {
                    SearchCursor searchCursorB = indexes.get(2).find(cursorK2, new long[]{pathK2, resultA[2]});
                    while (searchCursorB.hasNext(cursorK2)) {
                        resultB = searchCursorB.next(cursorK2);
                        if (resultA[1] == resultB[3]) {

                            count++;

                            //pause clock
                            clock = System.nanoTime();
                            indexes.get(3).insert(new long[]{pathID3, resultA[1], resultA[2], resultB[2], resultB[3]});
                            duration += System.nanoTime() - clock;
                            //start clock again
                        }

                    }
                }
            }
        }
        System.out.println("k3 joins found: " + count);
        return pathID3;
    }

    public long joinOnK1(long pathID1, long pathID2) throws IOException {
        long[] resultB;
        long clock;
        int count = 0;
        List<long[]> entries = new ArrayList<>();
        long[] key = new long[]{pathID1, pathID2};
        if(!k2PathIds.containsKey(key))
            k2PathIds.put(key, k2PathCounter++);
        long pathID3 = k2PathIds.get(key);
        try (PageProxyCursor cursor = disks.get(1).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
            SearchCursor searchCursorA = indexes.get(1).find(cursor, new long[]{pathID1});
            while (searchCursorA.hasNext(cursor)) {
                entries.add(searchCursorA.next(cursor));
            }
            for (long[] resultA : entries) {
                SearchCursor searchCursorB = indexes.get(1).find(cursor, new long[]{pathID2, resultA[2]});
                while (searchCursorB.hasNext(cursor)) {
                    resultB = searchCursorB.next(cursor);
                    count++;

                    //pause clock
                    clock = System.nanoTime();
                    indexes.get(2).insert(new long[]{pathID3, resultA[1], resultA[2], resultB[2]});
                    duration += System.nanoTime() - clock;
                    //start clock again
                }
            }
        }
        System.out.println("k2 joins found: " + count);
        return pathID3;
    }

    public void findK2(long pathID) throws IOException {
        /*
        int count = 0;
        SearchCursor searchCursor = indexes.get(2).find(new long[]{pathID});
        try(PageProxyCursor cursor = disks.get(2).getCursor(searchCursor.pageID, PagedFile.PF_SHARED_LOCK)){
            while(searchCursor.hasNext(cursor)){
                searchCursor.next(cursor);
                count++;
            }
        }
        System.out.println("Found: " + count);
        */
    }

    public void findK3(long pathID) throws IOException {
        /*
        int count = 0;
        SearchCursor searchCursor = indexes.get(3).find(new long[]{pathID});
        try(PageProxyCursor cursor = disks.get(3).getCursor(searchCursor.pageID, PagedFile.PF_SHARED_LOCK)){
            while(searchCursor.hasNext(cursor)){
                searchCursor.next(cursor);
                count++;
            }
        }
        System.out.println("Found: " + count);
        */
    }

    public void loadK1Index() throws IOException {
        enumerateSingleEdges();
        k1Sorter.sort();
        indexes.put(1, buildIndex(k1Sorter));
        k1Sorter.getSortedDisk().pageCacheFile.renameTo(new File("workloadK1.db"));

    }

    private void enumerateSingleEdges() throws IOException {
        int count = 0;
        double totalRels;
        try ( Transaction tx = database.beginTx()) {
            totalRels = IteratorUtil.count(ggo.getAllRelationships());
            for(Relationship edge : ggo.getAllRelationships()){
                addPath(edge.getStartNode(), edge, edge.getEndNode());
                addPath(edge.getEndNode(), edge, edge.getStartNode());
                count++;
            }
        }
        System.out.println("Keys written: " + count);
    }
    private void addPath(Node node1, Relationship relationship1, Node node2) throws IOException {
        PathIDBuilder builder = new PathIDBuilder(node1, relationship1, node2);
        long[] key = new long[]{builder.buildPath(), node1.getId(), node2.getId()};
        k1Sorter.addUnsortedKey(key);
        updateStats(builder);
    }

    public IndexTree buildIndex(Sorter sorter) throws IOException {
        DiskCache sortedDisk = sorter.getSortedDisk();
        disks.put(1, sortedDisk);
        IndexBulkLoader bulkLoader = new IndexBulkLoader(sortedDisk, sorter.finalPageId(), sorter.keySize);
        IndexTree index = bulkLoader.run();
        return index;
    }

    private void updateStats(PathIDBuilder builder){
        if(!relationshipMap.containsKey(builder.buildPath())){
            relationshipMap.put(builder.buildPath(), builder);
        }
    }
    public long nanoToMilli(long time){
        return time/1000000;
    }
}
