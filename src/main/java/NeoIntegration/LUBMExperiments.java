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
import java.util.ArrayList;
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

public class LUBMExperiments {
    public HashMap<Integer, IndexTree> indexes = new HashMap<>();
    public HashMap<Integer, DiskCache> disks = new HashMap<>();
    public GraphDatabaseService database;
    public GlobalGraphOperations ggo;
    StringBuilder stringBuilder;
    StringBuilder stringBuilder456;
    String cypher;

    public static void main(String[] args) throws IOException {
        LUBMExperiments experiments = new LUBMExperiments();
        int query;
        int index;

        //experiments.stats();


        int experimentCount = 5;
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
                if(args[1].equals("6")){
                    experiments.doExperiment6();
                }
                if(args[1].equals("7")){
                    experiments.doExperiment7();
                }
                if(args[1].equals("8")){
                    experiments.doExperiment8();
                }
                if(args[1].equals("9")){
                    experiments.doExperiment9();
                }
                if(args[1].equals("10")){
                    experiments.doExperiment10();
                }
                experiments.stringBuilder.append("\n");
            }
            else{
                System.out.println("Argument mismatch");
            }
        }

        experiments.logToFile();
        experiments.logToFile456();

        for(DiskCache disk : experiments.disks.values()){
            disk.shutdown();
        }
    }

    public LUBMExperiments() throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("LUBM50IndexCompressed/pathIndexMetaData.dat")));
        String line;
        while((line = bufferedReader.readLine()) != null) {
            List<String> entry = Arrays.asList(line.split(","));
            int k = new Integer(entry.get(0));
            long root = new Long(entry.get(1));
            boolean compressed = new Boolean(entry.get(2));
            DiskCache disk = DiskCache.persistentDiskCache("LUBM50IndexCompressed/K"+k+"Compressedlubm50Index.db", compressed);
            indexes.put(k, new IndexTree(k+1, root, disk));
            disks.put(k, disk);
        }
        bufferedReader.close();

        stringBuilder = new StringBuilder();
        stringBuilder456 = new StringBuilder();

        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(CleverIndexBuilder.DB_PATH).newGraphDatabase();

        ggo = GlobalGraphOperations.at(database);
    }


    public void doExperiment1() throws IOException {
        //query("MATCH (x)-[:memberOf]->(y) RETURN ID(x), ID(y)");
        //index(3, 649439727, null);
    }
    public void doExperiment2() throws IOException {
        //query("MATCH (x)-[:memberOf]->(y) WHERE x.uri=\"http://www.Department0.University0.edu/UndergraduateStudent207\" RETURN ID(x), ID(y)");
        //index(3, 649439727, new IndexConstraint(1, "uri", "http://www.Department0.University0.edu/UndergraduateStudent207"));
    }
    public void doExperiment3() throws IOException {
        //query("MATCH (x)-[:worksFor]->(y) RETURN ID(x), ID(y)");
        //index(3, 35729895, null);
    }
    public void doExperiment4() throws IOException {
        query("MATCH (x)-[:takesCourse]->(y)<-[:teacherOf]-(z) RETURN ID(x), ID(y), ID(z)");
        //index(4, 165, null);
        indexForQueries456(939155463, 1653142233);
    }
    public void doExperiment5() throws IOException {
        query("MATCH (x)-[:memberOf]->(y)<-[:subOrganizationOf]-(z) RETURN ID(x), ID(y), ID(z)");
        //index(4, 66, null);
        indexForQueries456(649439727, 1522104310);
    }
    public void doExperiment6() throws IOException {
        query("MATCH (x)-[:memberOf]->(y)-[:subOrganizationOf]->(z) RETURN ID(x), ID(y), ID(z)");
        //index(4, 69, null);
        indexForQueries456(649439727, 1190990026);
    }
    public void doExperiment7() throws IOException {
        query("MATCH (x)-[:undergraduateDegreeFrom]->(y)<-[:subOrganizationOf]-(z)<-[:memberOf]-(x) RETURN ID(x), ID(y), ID(z)");
        rectangleJoin(3, 1918060825, 4, 41);
        //indexShape(5, 856, null);
    }
    public void doExperiment8() throws IOException {
        query("MATCH (x)-[:hasAdvisor]->(y)-[:teacherOf]->(z)<-[:takesCourse]-(x) RETURN ID(x), ID(y), ID(z)");
        rectangleJoin(3, 901063622, 4, 140);
        //indexShape(5, 802, null);
    }
    public void doExperiment9() throws IOException {
        query("MATCH (x)<-[:headOf]-(y)-[:worksFor]->(z)<-[:subOrganizationOf]-(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        pathJoin(3, 1298760183, 4, 14);
        //index(5, 567, null);
    }
    public void doExperiment10() throws IOException {
        query("MATCH (x)<-[:headOf]-(y)-[:worksFor]->(z)-[:subOrganizationOf]->(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        pathJoin(3, 1298760183, 4, 17);
        //index(5, 570, null);
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
            //System.out.println("Begin Neo4j Transaction");
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
            //System.out.println("Number of results found in Neo4j:" + count);
            stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
            stringBuilder.append((timeToLastResult - startTime) / (double) 1000000).append(",");
        }
        return count;
    }


    public int indexForQueries456(long pathID1, long pathID2) throws IOException {
        long startTime = System.nanoTime();
        long timeToLastResult = 0;
        long timeToFirstResult= 0;
        int count = 0;
        List<long[]> entries = new ArrayList<>();
        try (PageProxyCursor cursor = disks.get(3).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
            SearchCursor searchCursorA = indexes.get(3).find(cursor, new long[]{pathID1});
            while (searchCursorA.hasNext(cursor)) {
                entries.add(searchCursorA.next(cursor));
            }
            for (long[] resultA : entries) {
                SearchCursor searchCursorB = indexes.get(3).find(cursor, new long[]{pathID2, resultA[2]});
                while (searchCursorB.hasNext(cursor)) {
                    if(timeToFirstResult == 0)
                        timeToFirstResult = System.nanoTime();
                    searchCursorB.next(cursor);
                    count++;
                }
            }
            timeToLastResult = System.nanoTime();
        }
        stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
        stringBuilder.append((timeToLastResult - startTime) / (double) 1000000).append(",");
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
                if(foundKey[1] == foundKey[foundKey.length -1])
                    count++;

            }
            timeToLastResult = System.nanoTime();
        }
        stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
        stringBuilder.append((timeToLastResult - startTime) / (double) 1000000).append(",");;
        //System.out.println("Index w/ constraint Join");
        //System.out.println((timeToFirstResult - startTime) / (double) 1000000);
        //System.out.println((timeToLastResult - startTime) / (double) 1000000);
        //System.out.println("Result Set Size index: " + count);

        if(tx != null){
            tx.close();
        }
        return count;
    }


    public int index(int index, long pathID, IndexConstraint constraint) throws IOException {
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
                count++;
                if(constraint != null){
                    Node n = database.getNodeById(foundKey[constraint.indexInResultSet]);
                    String value = (String)n.getProperty(constraint.property);
                    if(value.equals(constraint.value)){
                        count = 1;
                        break;
                    }
                }
            }
            timeToLastResult = System.nanoTime();
        }
        //System.out.println("Number of results found in Index: " + count);
        stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
        stringBuilder.append((timeToLastResult - startTime) / (double) 1000000);
        //System.out.println("Result Set Size index: " + count);

        if(tx != null){
            tx.close();
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
        SearchCursor searchCursorB = indexes.get(indexB).find(searchKeyB);
        try (PageProxyCursor cursorA = disks.get(indexA).getCursor(searchCursorA.pageID, PagedFile.PF_SHARED_LOCK)) {
            try (PageProxyCursor cursorB = disks.get(indexB).getCursor(searchCursorB.pageID, PagedFile.PF_SHARED_LOCK)) {
                timeToFirstResult = System.nanoTime();
                if(searchCursorA.hasNext(cursorA) && searchCursorB.hasNext(cursorB)) {
                    resultA = searchCursorA.next(cursorA);
                    resultB = searchCursorB.next(cursorB);
                    while (searchCursorA.hasNext(cursorA) && searchCursorB.hasNext(cursorB)) {
                        if (resultA[1] == resultB[1]) {
                            if(resultA[2] == resultB[3]) {
                                count++;
                                resultA = searchCursorA.next(cursorA);
                                resultB = searchCursorB.next(cursorB);
                            }
                            else if(resultA[2] > resultB[3]){
                                if (searchCursorB.hasNext(cursorB)) {
                                    resultB = searchCursorB.next(cursorB);
                                }
                            }
                            else{
                                if (searchCursorA.hasNext(cursorA)) {
                                    resultA = searchCursorA.next(cursorA);
                                }
                            }
                        } else if (resultA[1] > resultB[1]) {
                            if (searchCursorB.hasNext(cursorB)) {
                                resultB = searchCursorB.next(cursorB);
                            }
                        } else {
                            if (searchCursorA.hasNext(cursorA)) {
                                resultA = searchCursorA.next(cursorA);
                            }
                        }
                    }
                }
                timeToLastResult = System.nanoTime();
            }
        }
        //System.out.println("Number of results found in Index: " + count);
        stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
        stringBuilder.append((timeToLastResult - startTime) / (double) 1000000);
        //System.out.println("Result Set Size index: " + count);
        return count;
    }
    public int pathJoin(int indexA, long pathIDA, int indexB, long pathIDB) throws IOException {
        long startTime = System.nanoTime();
        long timeToFirstResult;
        long timeToLastResult;
        long[] searchKeyA = new long[]{pathIDA};
        long[] searchKeyB = new long[]{pathIDB};

        long[] resultA;
        long[] resultB;
        int count = 0;
        SearchCursor searchCursorA = indexes.get(indexA).find(searchKeyA);
        SearchCursor searchCursorB = indexes.get(indexB).find(searchKeyB);
        try (PageProxyCursor cursorA = disks.get(indexA).getCursor(searchCursorA.pageID, PagedFile.PF_SHARED_LOCK)) {
            try (PageProxyCursor cursorB = disks.get(indexB).getCursor(searchCursorB.pageID, PagedFile.PF_SHARED_LOCK)) {
                timeToFirstResult = System.nanoTime();
                if(searchCursorA.hasNext(cursorA) && searchCursorB.hasNext(cursorB)) {
                    resultA = searchCursorA.next(cursorA);
                    resultB = searchCursorB.next(cursorB);
                    while (searchCursorA.hasNext(cursorA) && searchCursorB.hasNext(cursorB)) {
                        if (resultA[1] == resultB[1]) {
                            count++;
                            //resultA = searchCursorA.next(cursorA);
                            resultB = searchCursorB.next(cursorB);
                        } else if (resultA[1] > resultB[1]) {
                            if (searchCursorB.hasNext(cursorB)) {
                                resultB = searchCursorB.next(cursorB);
                            }
                        } else {
                            if (searchCursorA.hasNext(cursorA)) {
                                resultA = searchCursorA.next(cursorA);
                            }
                        }
                    }
                }
                timeToLastResult = System.nanoTime();
            }
        }
        stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
        stringBuilder.append((timeToLastResult - startTime) / (double) 1000000);
        //System.out.println("Result Set Size index: " + count);
        return count;
    }

    public void logToFile(){
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("LUBMExperimentsCompressed_results.txt", true)))) {
            out.println(this.cypher+"\n");
            out.println(stringBuilder.toString());
            System.out.println(stringBuilder.toString());
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }
    public void logToFile456(){
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("LUBMExperimentsCompressed456_results.txt", true)))) {
            out.println(this.cypher+"\n");
            out.println(stringBuilder.toString());
            System.out.println(stringBuilder.toString());
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }


}

