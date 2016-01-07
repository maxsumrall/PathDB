/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.IndexTree;
import bptree.impl.SearchCursor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.tooling.GlobalGraphOperations;

public class AdvogadoBenchmarkExperiment {
    public static String DB_PATH = "graph.db";
    public HashMap<Integer, IndexTree> indexes = new HashMap<>();
    public HashMap<Integer, DiskCache> disks = new HashMap<>();
    public GraphDatabaseService database;
    public GlobalGraphOperations ggo;
    StringBuilder stringBuilder;
    String cypher;

    public static void main(String[] args) throws IOException {
        AdvogadoBenchmarkExperiment experiments = new AdvogadoBenchmarkExperiment();
        int query;
        int index;

        //experiments.stats();


        int experimentCount = 1;
        for(int i = 0; i < experimentCount; i++) {
            if(args.length == 2){
                System.out.println(args[0] + " " + args[1]);
                if(args[1].equals("1")){
                    experiments.doExperiment1();
                }
                if(args[1].equals("2")){
                    experiments.doExperiment2();
                }
                if(args[1].equals("3")){
                    experiments.doExperiment3();
                }
                if(args[1].equals("4")){
                    experiments.doExperiment4();
                }
                if(args[1].equals("5")){
                    experiments.doExperiment5();
                }

                experiments.stringBuilder.append("\n");
            }
            else{
                //System.out.println("Argument mismatch");
                experiments.doExperiment1();
                experiments.doExperiment2();
                experiments.doExperiment3();
                experiments.doExperiment4();
                experiments.doExperiment5();
            }
        }

        experiments.logToFile();

        for(DiskCache disk : experiments.disks.values()){
            disk.shutdown();
        }
    }

    public AdvogadoBenchmarkExperiment() throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("advogado2/pathIndexMetaData.dat")));
        String line;
        String folder = "advogado2/";
        while((line = bufferedReader.readLine()) != null) {
            List<String> entry = Arrays.asList(line.split(","));
            int k = new Integer(entry.get(0));
            long root = new Long(entry.get(1));
            boolean compressed = new Boolean(entry.get(2));
            DiskCache disk = DiskCache.persistentDiskCache(folder + "K"+k+"Cleverlubm50Index.db", compressed);
            indexes.put(k, new IndexTree(k+1, root, disk));
            disks.put(k, disk);
        }
        bufferedReader.close();

        stringBuilder = new StringBuilder();

        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DB_PATH).newGraphDatabase();

        ggo = GlobalGraphOperations.at(database);
    }


    public void doExperiment1() throws IOException {
        query("MATCH (x)-[:apprentice]->(y)-[:apprentice]->(z)-[:apprentice]->(x) RETURN ID(x), ID(y), ID(z)");
        indexShape(5, 252, null);
        rectangleJoin(3, 560747377, 4, 36);
    }
    public void doExperiment2() throws IOException {
        query("MATCH (x)-[:journeyer]->(y)-[:journeyer]->(z)-[:journeyer]->(x) RETURN ID(x), ID(y), ID(z)");
        indexShape(5, 166, null);
        rectangleJoin(3, 1693961325, 4, 22);
    }
    public void doExperiment3() throws IOException {
        query("MATCH (x)-[:master]->(y)-[:master]->(z)-[:master]->(x) RETURN ID(x), ID(y), ID(z)");
        indexShape(5, 209, null);
        rectangleJoin(3, 1081267614, 4, 29);
    }
    public void doExperiment4() throws IOException {
        query("MATCH (x)-[:apprentice]->(y)-[:journeyer]->(z)-[:master]->(x) RETURN ID(x), ID(y), ID(z)");
        indexShape(5, 239, null);
        rectangleJoin(3, 560747377, 4, 23);
    }
    public void doExperiment5() throws IOException {
        query("MATCH (x)-[:apprentice]->(y)-[:apprentice]->(z)-[:master]->(x) RETURN ID(x), ID(y), ID(z)");
        indexShape(5, 251, null);
        rectangleJoin(3, 560747377, 4, 35);
    }

    public void stats(){
        int totalRels;
        int totalNodes;
        long degreeCount = 0;
        try(Transaction tx = database.beginTx()){
            totalRels = IteratorUtil.count(ggo.getAllRelationships());
            totalNodes = IteratorUtil.count(ggo.getAllNodes());
            for(Node node : ggo.getAllNodes()){
                degreeCount += node.getDegree();
            }
        }

        double averageDegree = degreeCount / (double) totalNodes;
        stringBuilder.append("Total nodes: ").append(totalNodes);
        stringBuilder.append("Total edges: ").append(totalRels);
        stringBuilder.append("Average degree per node: ").append(averageDegree);
    }

    public int query(String cypher){
        int count = 0;
        this.cypher = cypher;
        try(Transaction tx = database.beginTx()){
            for(RelationshipType each : ggo.getAllRelationshipTypes()){
                //System.out.println(each);
            }
            //stringBuilder.append("\n").append(cypher);
            System.out.println(cypher);
            long startTime = System.nanoTime();
            Result queryAResult = database.execute(cypher);
            queryAResult.next();
            long timeToFirstResult = System.nanoTime();
            //System.out.println("Query Statistics:\n" + queryAResult.getQueryStatistics());
            //System.out.println("Query Execution Plan Description:\n" + queryAResult.getExecutionPlanDescription());

            while(queryAResult.hasNext()){
                //System.out.println(queryAResult.next().toString());
                queryAResult.next();
                count++;
            }
            long timeToLastResult = System.nanoTime();
            System.out.println("Number of results found in Neo4j:" + count);
            stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
            stringBuilder.append((timeToLastResult - startTime) / (double) 1000000).append(",");
        }
        return count;
    }

    public int rectangleJoin(int indexA, long pathIDA, int indexB, long pathIDB) throws IOException {
        long startTime = System.nanoTime();
        long timeToFirstResult;
        long timeToLastResult;
        long[] searchKeyA = new long[]{pathIDA};
        long[] searchKeyB = new long[]{pathIDB};

        long[] resultA;
        long[] resultB;
        int count = 0;
        SearchCursor searchCursorA = indexes.get(indexA).find(searchKeyA);
        try (PageProxyCursor cursorA = disks.get(indexA).getCursor(searchCursorA.pageID, PagedFile.PF_SHARED_LOCK)) {
            try (PageProxyCursor cursorB = disks.get(indexB).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
                timeToFirstResult = System.nanoTime();
                while (searchCursorA.hasNext(cursorA)) {
                resultA = searchCursorA.next(cursorA);
                    SearchCursor searchCursorB = indexes.get(indexB).find(cursorB, new long[]{pathIDB, resultA[2]});
                    resultB = searchCursorB.next(cursorB);
                    while (searchCursorB.hasNext(cursorB)) {
                        if (resultA[1] == resultB[3]) {
                            count++;
                        }
                        resultB = searchCursorB.next(cursorB);
                    }
                }
                timeToLastResult = System.nanoTime();
            }
        }
        //System.out.println("Number of results found in Index: " + count);
        stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
        stringBuilder.append((timeToLastResult - startTime) / (double) 1000000);
        System.out.println("Result Set Size index: " + count);
        return count;
    }


    public int indexShape(int index, long pathID, IndexConstraint constraint) throws IOException {
        Transaction tx = null;
        if(constraint != null){
            tx = database.beginTx();
        }
        long startTime = System.nanoTime();
        long timeToFirstResult;
        long timeToLastResult;
        long[] searchKey = new long[]{pathID};

        long[] foundKey;
        int count = 0;
        SearchCursor searchCursor = indexes.get(index).find(searchKey);
        try (PageProxyCursor cursor = disks.get(index).getCursor(searchCursor.pageID, PagedFile.PF_SHARED_LOCK)) {
            timeToFirstResult = System.nanoTime();
            while(searchCursor.hasNext(cursor)) {
                foundKey = searchCursor.next(cursor);
                if(foundKey[1] == foundKey[foundKey.length -1]) {
                    count++;
                }

            }
            timeToLastResult = System.nanoTime();
        }
        stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
        stringBuilder.append((timeToLastResult - startTime) / (double) 1000000).append(",");;
        //System.out.println("Index w/ constraint Join");
        //System.out.println((timeToFirstResult - startTime) / (double) 1000000);
        //System.out.println((timeToLastResult - startTime) / (double) 1000000);
        System.out.println("Result Set Size index: " + count);

        if(tx != null){
            tx.close();
        }
        return count;
    }

    public void logToFile(){
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("AdvogadoExperiments_results.txt", true)))) {
            out.println(this.cypher+"\n");
            out.println(stringBuilder.toString());
            System.out.println(stringBuilder.toString());
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }

}

