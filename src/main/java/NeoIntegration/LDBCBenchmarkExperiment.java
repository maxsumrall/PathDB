package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.IndexTree;
import bptree.impl.SearchCursor;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by max on 6/21/15.
 */
public class LDBCBenchmarkExperiment {
    public HashMap<Integer, IndexTree> indexes = new HashMap<>();
    public HashMap<Integer, DiskCache> disks = new HashMap<>();
    public GraphDatabaseService database;
    public GlobalGraphOperations ggo;
    StringBuilder stringBuilder;
    StringBuilder stringBuilder456;
    String cypher;
    public static String DB_PATH = "graph.db";

    public static void main(String[] args) throws IOException {
        LDBCBenchmarkExperiment experiments = new LDBCBenchmarkExperiment();
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
                    experiments.doExperiment4();
                }
                if(args[1].equals("4")){
                    experiments.doExperiment5();
                }
                if(args[1].equals("5")){
                    experiments.doExperiment7();
                }
                if(args[1].equals("6")){
                    experiments.doExperiment8();
                }
                if(args[1].equals("7")){
                    experiments.doExperiment10();
                }
                if(args[1].equals("8")){
                    experiments.doExperiment11();
                }
                experiments.stringBuilder.append("\n");
            }
            else{
                System.out.println("Argument mismatch");
                experiments.doExperiment1();
                experiments.doExperiment2();
                experiments.doExperiment4();
                experiments.doExperiment5();
                experiments.doExperiment7();
                experiments.doExperiment8();
                experiments.doExperiment10();
                experiments.doExperiment11();
            }
        }

        experiments.logToFile();

        for(DiskCache disk : experiments.disks.values()){
            disk.shutdown();
        }
    }

    public LDBCBenchmarkExperiment() throws IOException {




        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("LDBC/pathIndexMetaData.dat")));
        String line;
        while((line = bufferedReader.readLine()) != null) {
            List<String> entry = Arrays.asList(line.split(","));
            int k = new Integer(entry.get(0));
            long root = new Long(entry.get(1));
            boolean compressed = new Boolean(entry.get(2));
            DiskCache disk;
            if(k == 5)
                disk = DiskCache.persistentDiskCache("/Volumes/Passport/K"+k+"LDBCIndex.db", compressed);
            else
                disk = DiskCache.persistentDiskCache("LDBC/K"+k+"LDBCIndex.db", compressed);
            indexes.put(k, new IndexTree(k+1, root, disk));
            disks.put(k, disk);
        }
        bufferedReader.close();

        stringBuilder = new StringBuilder();
        stringBuilder456 = new StringBuilder();

        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DB_PATH).newGraphDatabase();

        ggo = GlobalGraphOperations.at(database);
    }


    public void doExperiment1() throws IOException {
        query("MATCH (x)-[:KNOWS]->(y)-[:PERSON_IS_LOCATED_IN]->(z) RETURN ID(x), ID(y), ID(z)");
        //index(4, 171, null);
        indexForQueriesXYZ(71666472, 119564259);
    }
    public void doExperiment2() throws IOException {
        query("MATCH (x)-[:KNOWS]->(y)<-[:POST_HAS_CREATOR]-(z) RETURN ID(x), ID(y), ID(z)");
        //index(4, 181, null);
        indexForQueriesXYZ(71666472, 51381928);
    }

    public void doExperiment4() throws IOException {
        query("MATCH (x)-[:KNOWS]->(y)<-[:POST_HAS_CREATOR]-(z)-[:POST_HAS_TAG]->(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        //index(5, 1455, null);
        indexForQueriesXYZW(3,71666472, 4, 252 );
    }
    public void doExperiment5() throws IOException {
        query("MATCH (x)-[:KNOWS]->(y)<-[:HAS_MEMBER]-(z) RETURN ID(x), ID(y), ID(z)");
        //index(4, 173, null);
        indexForQueriesXYZ(71666472, 1634561761);
    }
    public void doExperiment7() throws IOException {
        query("MATCH (x)<-[:POST_HAS_CREATOR]-(y)<-[:LIKES_POST]-(z) RETURN ID(x), ID(y), ID(z)");
        //index(5, 257, null);
        indexForQueriesXYZ(51381928, 2005334717);
    }
    public void doExperiment8() throws IOException {
        query("MATCH (x)<-[:POST_HAS_CREATOR]-(y)<-[:REPLY_OF_POST]-(z)-[:COMMENT_HAS_CREATOR]->(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        //index(5, 1942, null);
        indexForQueriesXYZW(3, 51381928, 4, 79);
    }
    public void doExperiment10() throws IOException {
        query("MATCH (x)-[:KNOWS]->(y)-[:KNOWS]->(z)-[:PERSON_IS_LOCATED_IN]->(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        //index(5, 1434, null);
        indexForQueriesXYZW(3, 71666472, 4, 171);
    }
    public void doExperiment11() throws IOException {
        query("MATCH (x)-[:KNOWS]->(y)-[:WORKS_AT]->(z)-[:ORGANISATION_IS_LOCATED_IN]->(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        //index(5, 1453, null);
        indexForQueriesXYZW(3,71666472, 4,  238);
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


    public int indexForQueriesXYZ(long pathID1, long pathID2) throws IOException {
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
    public int indexForQueriesXYZW(int index1, long pathID1, int index2, long pathID2) throws IOException {
        long startTime = System.nanoTime();
        long timeToLastResult = 0;
        long timeToFirstResult= 0;
        int count = 0;
        List<long[]> entries = new ArrayList<>();
        try (PageProxyCursor cursorA = disks.get(index1).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
            try (PageProxyCursor cursorB = disks.get(index2).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
                SearchCursor searchCursorA = indexes.get(index1).find(cursorA, new long[]{pathID1});
                while (searchCursorA.hasNext(cursorA)) {
                    entries.add(searchCursorA.next(cursorA));
                }
                for (long[] resultA : entries) {
                    SearchCursor searchCursorB = indexes.get(index2).find(cursorB, new long[]{pathID2, resultA[resultA.length -1]});
                    while (searchCursorB.hasNext(cursorB)) {
                        if (timeToFirstResult == 0)
                            timeToFirstResult = System.nanoTime();
                        searchCursorB.next(cursorB);
                        count++;
                    }
                }
                timeToLastResult = System.nanoTime();
            }
        }
        stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
        stringBuilder.append((timeToLastResult - startTime) / (double) 1000000).append(",");
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
        System.out.println("Number of results found in Index: " + count);
        stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
        stringBuilder.append((timeToLastResult - startTime) / (double) 1000000);

        if(tx != null){
            tx.close();
        }
        return count;
    }


    public void logToFile(){
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("LDBC-Alt-Experiments_results.txt", true)))) {
            out.println(this.cypher+"\n");
            out.println(stringBuilder.toString());
            System.out.println(stringBuilder.toString());
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }
}

