package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.IndexTree;
import bptree.impl.SearchCursor;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
class simpleLUBMExperiments {
    public HashMap<Integer, IndexTree> indexes = new HashMap<>();
    public HashMap<Integer, DiskCache> disks = new HashMap<>();
    public GraphDatabaseService database;
    public GlobalGraphOperations ggo;
    StringBuilder stringBuilder;
    String cypher;

    public static void main(String[] args) throws IOException {
        simpleLUBMExperiments experiments = new simpleLUBMExperiments();
        int query;
        int index;

/*
        query = experiments.query("MATCH (x)-[:memberOf]->(y) RETURN ID(x), ID(y)");
        index = experiments.index(3, 649439727, null);

        query = experiments.query("MATCH (x)-[:memberOf]->(y) WHERE x.uri=\"http://www.Department0.University0.edu/UndergraduateStudent207\" RETURN ID(x), ID(y)");
        index = experiments.index(3, 649439727, new IndexConstraint(1, "uri", "http://www.Department0.University0.edu/UndergraduateStudent207"));

        query = experiments.query("MATCH (x)-[:worksFor]->(y) RETURN ID(x), ID(y)");
        index = experiments.index(3, 35729895, null);
*/
        query = experiments.query("MATCH (x)-[:takesCourse]->(y)<-[:teacherOf]-(z) RETURN ID(x), ID(y), ID(z)");
        //index = experiments.index(4, 165, null);
        index = experiments.indexForQueries456(939155463, 1653142233);


        query = experiments.query("MATCH (x)-[:memberOf]->(y)<-[:subOrganizationOf]-(z) RETURN ID(x), ID(y), ID(z)");
        //index = experiments.index(4, 66, null);
        index = experiments.indexForQueries456(649439727, 1522104310);

        query = experiments.query("MATCH (x)-[:memberOf]->(y)-[:subOrganizationOf]->(z) RETURN ID(x), ID(y), ID(z)");
        //index = experiments.index(4, 69, null);
        index = experiments.indexForQueries456(649439727, 1190990026);

        //query = experiments.query("MATCH (x)-[:undergraduateDegreeFrom]->(y)<-[:subOrganizationOf]-(z)<-[:memberOf]-(x) RETURN ID(x), ID(y), ID(z)");
        //query = experiments.query("MATCH (x)-[:undergraduateDegreeFrom]->(y)<-[:subOrganizationOf]-(z)<-[:memberOf]-(x) RETURN ID(x), ID(y), ID(z)");
        //index = experiments.rectangleJoin(3, 1918060825, 4, 49);

        //index = experiments.indexShape(5, 323, null);
        //index = experiments.indexShape(5, 856, null);

/*
        query = experiments.query("MATCH (x)-[:hasAdvisor]->(y)-[:teacherOf]->(z)<-[:takesCourse]-(x) RETURN ID(x), ID(y), ID(z)");
        //index = experiments.rectangleJoin(3, 939155463, 4, 57);

        index = experiments.indexShape(5, 802, null);

        query = experiments.query("MATCH (x)<-[:headOf]-(y)-[:worksFor]->(z)<-[:subOrganizationOf]-(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        //index = experiments.pathJoinAlpha(3, 1221271593, 4, 4);

        index = experiments.index(5, 567, null);

        query = experiments.query("MATCH (x)<-[:headOf]-(y)-[:worksFor]->(z)-[:subOrganizationOf]->(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        //index = experiments.pathJoin(3, 1221271593, 4, 1);

        index = experiments.index(5, 570, null);
*/
        for(DiskCache disk : experiments.disks.values()){
            disk.shutdown();
        }

    }

    public simpleLUBMExperiments() throws IOException {

        String folder = "LUBM50IndexCompressed/";
        //String folder = "LUBM50Index/";
        //String folder = "LUBM50IndexLexicographic/";
        //String folder = "";

        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("LUBM50IndexCompressed/pathIndexMetaData.dat")));
        String line;
        while((line = bufferedReader.readLine()) != null) {
            List<String> entry = Arrays.asList(line.split(","));
            int k = new Integer(entry.get(0));
            long root = new Long(entry.get(1));
            boolean compressed = entry.get(2).equals("true");
            DiskCache disk = DiskCache.persistentDiskCache(folder +"K"+k+"Compressedlubm50Index.db", compressed);
            indexes.put(k, new IndexTree(k+1, root, disk));
            disks.put(k, disk);
        }
        bufferedReader.close();

        stringBuilder = new StringBuilder();

        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(CleverIndexBuilder.DB_PATH).newGraphDatabase();
        ggo = GlobalGraphOperations.at(database);
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
            System.out.println("Begin Neo4j Transaction");
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
            System.out.println((timeToFirstResult - startTime) / (double) 1000000);
            System.out.println((timeToLastResult - startTime) / (double) 1000000);
        }
        return count;
    }

    public int indexForQueries456(long pathID1, long pathID2) throws IOException {
        long startTime = System.nanoTime();
        long timeToLastResult = 0;
        long timeToFirstResult= 0;
        int count = 0;
        long[] resultA;
        try (PageProxyCursor cursor = disks.get(3).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
            SearchCursor searchCursorA = indexes.get(3).find(cursor, new long[]{pathID1});
            while (searchCursorA.hasNext(cursor)) {
                resultA = searchCursorA.next(cursor);
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
        System.out.println("Index456");
        System.out.println((timeToFirstResult - startTime) / (double) 1000000);
        System.out.println((timeToLastResult - startTime) / (double) 1000000);
        System.out.println("Result Set Size index: " + count);
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
        System.out.println("Index");
        System.out.println((timeToFirstResult - startTime) / (double) 1000000);
        System.out.println((timeToLastResult - startTime) / (double) 1000000);
        System.out.println("Result Set Size index: " + count);

        if(tx != null){
            tx.close();
        }
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
        System.out.println("Index w/ constraint Join");
        System.out.println((timeToFirstResult - startTime) / (double) 1000000);
        System.out.println((timeToLastResult - startTime) / (double) 1000000);
        System.out.println("Result Set Size index: " + count);

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
                                resultsToFile(Arrays.toString(resultA) + ", " + Arrays.toString(resultB));
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
        System.out.println("Rectangle Join");
        System.out.println((timeToFirstResult - startTime) / (double) 1000000);
        System.out.println((timeToLastResult - startTime) / (double) 1000000);
        System.out.println("Result Set Size index: " + count);
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
                            resultA = searchCursorA.next(cursorA);
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
        System.out.println("Path Join");
        System.out.println((timeToFirstResult - startTime) / (double) 1000000);
        System.out.println((timeToLastResult - startTime) / (double) 1000000);
        System.out.println("Result Set Size index: " + count);
        return count;
    }
    public int pathJoinAlpha(int indexA, long pathIDA, int indexB, long pathIDB) throws IOException {
        long startTime = System.nanoTime();
        long timeToFirstResult;
        long timeToLastResult;
        long[] searchKeyA = new long[]{pathIDA};

        long[] resultA;
        long[] resultB;
        int count = 0;
        SearchCursor searchCursorA = indexes.get(indexA).find(searchKeyA);
        try (PageProxyCursor cursorA = disks.get(indexA).getCursor(searchCursorA.pageID, PagedFile.PF_SHARED_LOCK)) {
            while(searchCursorA.hasNext(cursorA)) {
                resultA = searchCursorA.next(cursorA);
                long[] searchKeyB = new long[]{pathIDB, resultA[1]};
                SearchCursor searchCursorB = indexes.get(indexB).find(searchKeyB);
                try (PageProxyCursor cursorB = disks.get(indexB).getCursor(searchCursorB.pageID, PagedFile.PF_SHARED_LOCK)) {
                        while (searchCursorB.hasNext(cursorB)) {
                            resultB = searchCursorB.next(cursorB);
                                count++;
                            }
                        }
                    }
                }
        //stringBuilder.append((timeToFirstResult - startTime) / (double) 1000000).append(",");
        //stringBuilder.append((timeToLastResult - startTime) / (double) 1000000);
        System.out.println("Result Set Size index: " + count);
        return count;
    }


    public void logToFile(){
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("LUBMExperiments_results.txt", true)))) {
            out.println(this.cypher+"\n");
            out.println(stringBuilder.toString());
            System.out.println(stringBuilder.toString());
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }
    public void resultsToFile(String result){
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("rectangleJoin.txt", true)))) {
            //out.println(result + "\n");
            //System.out.println(stringBuilder.toString());
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }


}
