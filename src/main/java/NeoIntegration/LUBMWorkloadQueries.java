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

/**
 * Created by max on 6/18/15.
 */
public class LUBMWorkloadQueries {
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
        LUBMWorkloadQueries workload = new LUBMWorkloadQueries();
        workload.run();
        workload.disks.get(1).shutdown();
        workload.disks.get(2).shutdown();
        workload.disks.get(3).shutdown();

        System.out.println("Workload Index Size: " + (workload.disks.get(1).pageCacheFile.length() + workload.disks.get(2).pageCacheFile.length() + workload.disks.get(3).pageCacheFile.length()));

    }

    public LUBMWorkloadQueries() throws IOException {

        DiskCache diskK2 = DiskCache.persistentDiskCache("workloadK2.db", false);
        DiskCache diskK3 = DiskCache.persistentDiskCache("workloadK3.db", false);

        disks.put(2, diskK2);
        disks.put(3, diskK3);

        IndexTree indexK2 = new IndexTree(4, diskK2);
        IndexTree indexK3 = new IndexTree(5, diskK3);
        indexes.put(2, indexK2);
        indexes.put(3, indexK3);



        stringBuilder = new StringBuilder();

        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(CleverIndexBuilder.DB_PATH).newGraphDatabase();

        ggo = GlobalGraphOperations.at(database);
    }


    public void run() throws IOException {
        //Start query 1

        long startTime = System.nanoTime();

        loadK1Index();

        query4();

        query5();

        query6();

        query7A();

        query7B();

        query8A();

        query8B();

        query9();

        query10();

        System.out.println("Total Query Workload Time: " + nanoToMilli(System.nanoTime() - startTime));


    }

    public void query4() throws IOException {
        long pathID = 939155463l; //takesCourse->
        long pathID2 = 1653142233l; //<-teacherOf
        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathID, pathID2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;

        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 4: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 4: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 4: Total Time : " + nanoToMilli(total));
        System.out.println("Query 4: Subsequent Search: " + subsequentSearch);
    }

    public void query5() throws IOException {
        long pathID = 649439727l; //memberOf - >
        long pathID2 = 1522104310l; //<-subOrganizationOf
        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathID, pathID2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;

        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 5: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 5: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 5: Total Time : " + nanoToMilli(total));
        System.out.println("Query 5: Subsequent Search: " + subsequentSearch);
    }

    public void query6() throws IOException {
        long pathID = 649439727l; //memberOf - >
        long pathID2 = 1190990026l; //subOrganizationOf - >
        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathID, pathID2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 6: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 6: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 6: Total Time : " + nanoToMilli(total));
        System.out.println("Query 6: Subsequent Search: " + subsequentSearch);
    }


    public void query7A() throws IOException {
        long pathK1 = 1918060825l; // undergraduateDegreeFrom - >
        long[] key = new long[]{649439727l, 1190990026l};
        long pathK2 = -1; // <-memberOf
        for(long[] val : k2PathIds.keySet()){
            if(val[0] == key[0])
                if(val[1] == key[1])
                    pathK2 = k2PathIds.get(val);
        }
        duration = 0;

        long starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2Version1(pathK1, pathK2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 7A: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 7A: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 7A: Total Time : " + nanoToMilli(total));
        System.out.println("Query 7A: Subsequent Search: " + subsequentSearch);
    }

    public void query7B() throws IOException {
        long pathK1 = 1918060825l; // undergraduateDegreeFrom - >
        long pathK21 = 1522104310l; // <-subOrgOf
        long pathK22 = 2131368143; // <-memberOF

        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinOnK1(pathK21, pathK22);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathID3);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 7B-1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 7B-1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 7B-1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 7B-1: Subsequent Search: " + subsequentSearch);

        duration = 0;


        starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2Version3(pathK1, pathID3);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 7B-2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 7B-2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 7B-2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 7B-2: Subsequent Search: " + subsequentSearch);
    }

    public void query8A() throws IOException {
        long pathK1 = 901063622; // hasAdvisor - >
        long[] key = new long[]{939155463l, 1653142233l};
        long pathK2 = -1; // <-memberOf
        for (long[] val : k2PathIds.keySet()) {
            if (val[0] == key[0])
                if (val[1] == key[1])
                    pathK2 = k2PathIds.get(val);
        }
        duration = 0;

        long starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2Version1(pathK1, pathK2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 8A: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 8A: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 8A: Total Time : " + nanoToMilli(total));
        System.out.println("Query 8A: Subsequent Search: " + subsequentSearch);
    }

    public void query8B() throws IOException {
        long pathK1 = 901063622l; // hasAdvisor - >
        long pathK21 = 454138535l;// teacherOf - >
        long pathK22 = 1682423943l;// <-takesCourse


        duration = 0;

        long starttime = System.nanoTime();
        long pathK2 = joinOnK1(pathK21, pathK22);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathK2);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 8B-1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 8B-1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 8B-1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 8B-1: Subsequent Search: " + subsequentSearch);

        duration = 0;



        starttime = System.nanoTime();
        long pathK3 = joinOnK1AndK2Version3(pathK1, pathK2);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathK3);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 8B-2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 8B-2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 8B-2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 8B-2: Subsequent Search: " + subsequentSearch);


    }

    public void query9() throws IOException {
        long pathK1 = 1298760183; // <-headOf
        long pathK12 = 35729895; //worksFor->
        long pathK13 = 1522104310; // <-subOrgOf

        //do the first merge shit

        long starttime = System.nanoTime();
        long pathK2 = joinOnK1(pathK12, pathK13);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK2(pathK2);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 9-1: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 9-1: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 9-1: Total Time : " + nanoToMilli(total));
        System.out.println("Query 9-1: Subsequent Search: " + subsequentSearch);


        //Now complete this query by merging that last result


        duration = 0;

        starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2Version2(pathK1, pathK2);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;
        starttime = System.nanoTime();
        findK3(pathID4);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 9-2: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 9-2: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 9-2: Total Time : " + nanoToMilli(total));
        System.out.println("Query 9-2: Subsequent Search: " + subsequentSearch);
    }


    public void query10() throws IOException {
        long pathK1 = 1298760183; // <-headOf
        long pathK12 = 35729895; //worksFor->
        long pathK13 = 1190990026; // subOrgOf->

        //do the first merge shit

        long starttime = System.nanoTime();
        long pathK2 = joinOnK1(pathK12, pathK13);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;

        starttime = System.nanoTime();
        findK2(pathK2);
        long subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 10A: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 10A: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 10A: Total Time : " + nanoToMilli(total));
        System.out.println("Query 10A: Subsequent Search: " + subsequentSearch);


        //Now complete this query by merging that last result


        duration = 0;

        starttime = System.nanoTime();
        long pathID4 = joinOnK1AndK2Version2(pathK1, pathK2);
        endTime = System.nanoTime();
        total = endTime - starttime;
        totalWithoutInsertion = total - duration;

        starttime = System.nanoTime();
        findK3(pathID4);
        subsequentSearch = nanoToMilli(System.nanoTime() - starttime);

        System.out.println("Queyr 10B: insertion time : " + nanoToMilli(duration));
        System.out.println("Queyr 10B: finding time : " + nanoToMilli(totalWithoutInsertion));
        System.out.println("Query 10B: Total Time : " + nanoToMilli(total));
        System.out.println("Query 10B: Subsequent Search: " + subsequentSearch);
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
       /* int count = 0;
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
