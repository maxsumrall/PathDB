/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package NeoIntegration;


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

public class LDBCWorkloadExperiment {
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
    long query2 = 0;
    long query1 = 0;

    public static void main(String[] args) throws IOException {
        LDBCWorkloadExperiment workload = new LDBCWorkloadExperiment();
        workload.run();
        workload.disks.get(1).shutdown();
        workload.disks.get(2).shutdown();
        workload.disks.get(3).shutdown();

        System.out.println("Workload Index Size: " + (workload.disks.get(1).pageCacheFile.length() + workload.disks.get(2).pageCacheFile.length() + workload.disks.get(3).pageCacheFile.length()));
    }

    public LDBCWorkloadExperiment() throws IOException {

        DiskCache diskK2 = DiskCache.persistentDiskCache("LDBCworkloadK2.db", false);
        DiskCache diskK3 = DiskCache.persistentDiskCache("LDBCworkloadK3.db", false);

        disks.put(2, diskK2);
        disks.put(3, diskK3);

        IndexTree indexK2 = new IndexTree(4, diskK2);
        IndexTree indexK3 = new IndexTree(5, diskK3);
        indexes.put(2, indexK2);
        indexes.put(3, indexK3);

        stringBuilder = new StringBuilder();

        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(LDBCBenchmarkExperiment.DB_PATH).newGraphDatabase();

        ggo = GlobalGraphOperations.at(database);
    }


    public void run() throws IOException {
        //Start query 1

        long startTime = System.nanoTime();

        loadK1Index();

        query1();

        query2();

        query4();

        query5();

        query7();

        query8();

        query10();

        query11();


        System.out.println("Total Query Workload Time: " + nanoToMilli(System.nanoTime() - startTime));


    }

    public void query1() throws IOException {
        long pathK1 = 71666472; // KNOWS - >
        long pathK2 = 119564259; // Person is located in - >

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK1, pathK2);
        query1 = pathID3;
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 1: Subsequent Search: " + subsequentSearch);

        duration = 0;

    }


    public void query2() throws IOException {
        long pathK1 = 71666472; // KNOWS - >
        long pathK2 = 51381928; // < - post has creator

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK1, pathK2);
        query2 = pathID3;
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 2: Subsequent Search: " + subsequentSearch);

        duration = 0;

    }

    public void query4() throws IOException {
        long path2 = 357847958; //post has tag - >

        duration = 0;


        long starttime = System.nanoTime();
        long pathID4 = joinOnK2AndK1(query2, path2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 4: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 4: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 4: Total Time : " + nanoToMilli(total));
        System.out.println("Query 4: Subsequent Search: " + subsequentSearch);
    }

    public void query5() throws IOException {
        long pathK1 = 71666472; // KNOWS - >
        long pathK2 = 1634561761; // < - has member

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK1, pathK2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 5: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 5: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 5: Total Time : " + nanoToMilli(total));
        System.out.println("Query 5: Subsequent Search: " + subsequentSearch);

        duration = 0;

    }

    public void query7() throws IOException {
        long pathK1 = 51381928; // < -post has creator
        long pathK2 = 2005334717; // < - likes post

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK1, pathK2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 7: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 7: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 7: Total Time : " + nanoToMilli(total));
        System.out.println("Query 7: Subsequent Search: " + subsequentSearch);

        duration = 0;

    }

    public void query8() throws IOException {
        long pathK1 = 1917276883; // < - reply of post
        long pathK21 = 1375264441; // comment has creator - >
        long pathK22 = 51381928; //< - post has creator

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK1, pathK21);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 8-1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 8-1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 8-1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 8-1: Subsequent Search: " + subsequentSearch);

        duration = 0;


        starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2(pathK22, pathID3);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 8-2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 8-2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 8-2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 8-2: Subsequent Search: " + subsequentSearch);
    }


    public void query10() throws IOException {
        long pathK1 = 71666472; // knows - >

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1AndK2(pathK1, query1);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 10: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 10: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 10: Total Time : " + nanoToMilli(total));
        System.out.println("Query 10: Subsequent Search: " + subsequentSearch);

    }



    public void query11() throws IOException {
        long pathK1 = 71666472; // knows - >
        long pathK21 = 818411056; // works at - >
        long pathK22 = 344548578; //organization is located in - >

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK21, pathK22);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Query 11-1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 11-1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 11-1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 11-1: Subsequent Search: " + subsequentSearch);

        duration = 0;


        starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2(pathK1, pathID3);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 11-2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 11-2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 11-2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 11-2: Subsequent Search: " + subsequentSearch);
    }



    public long joinOnK2AndK1(long pathK2, long pathK1) throws IOException {
        long[] resultA;
        long clock;
        int count = 0;
        List<long[]> entries = new ArrayList<>();
        long[] key = new long[]{pathK1, pathK2};
        if(!k3PathIds.containsKey(key))
            k3PathIds.put(key, k3PathCounter++);
        long pathID3 = k3PathIds.get(key);
        try (PageProxyCursor cursorK1 = disks.get(1).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
            try (PageProxyCursor cursorK2 = disks.get(2).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
                SearchCursor searchCursorB = indexes.get(2).find(cursorK2, new long[]{pathK2});
                while (searchCursorB.hasNext(cursorK2)) {
                    entries.add(searchCursorB.next(cursorK2));
                }
                for (long[] resultB : entries) {
                    SearchCursor searchCursorA = indexes.get(1).find(cursorK1, new long[]{pathK1, resultB[3]});
                    while (searchCursorA.hasNext(cursorK1)) {
                        resultA = searchCursorA.next(cursorK1);

                        count++;

                        //pause clock
                        clock = System.nanoTime();
                        indexes.get(3).insert(new long[]{pathID3, resultB[1], resultB[2], resultB[3], resultA[2]});
                        duration += System.nanoTime() - clock;
                        //start clock again

                    }
                }
            }
        }
        System.out.println("k3 joins found: " + count);
        return pathID3;
    }

    public long joinOnK1AndK2(long pathK1, long pathK2) throws IOException {
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
        k1Sorter.getSortedDisk().pageCacheFile.renameTo(new File("LDBCworkloadK1.db"));

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
