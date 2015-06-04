package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.NodeTree;
import bptree.impl.SearchCursor;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by max on 5/28/15.
 */
public class LUBMExperiments {
    public HashMap<Integer, NodeTree> indexes = new HashMap<>();
    public HashMap<Integer, DiskCache> disks = new HashMap<>();
    public GraphDatabaseService database;
    public GlobalGraphOperations ggo;

    public static void main(String[] args) throws IOException {
        LUBMExperiments experiments = new LUBMExperiments();
        int query;
        int index;

/*

        query = experiments.query("MATCH (x)-[:memberOf]->(y) RETURN ID(x), ID(y)");
        index = experiments.index(3, 649439727, null);
        assert(query == index);


        query = experiments.query("MATCH (x)-[:memberOf]->(y) WHERE x.uri=\"http://www.Department0.University0.edu/UndergraduateStudent207\" RETURN ID(x), ID(y)");
        index = experiments.index(3, 649439727, new IndexConstraint(1, "uri", "http://www.Department0.University0.edu/UndergraduateStudent207"));
        assert(query == index);

        query = experiments.query("MATCH (x)-[:worksFor]->(y) RETURN ID(x), ID(y)");
        index = experiments.index(3, 35729895, null);
        assert(query == index);

        query = experiments.query("MATCH (x)-[:takesCourse]->(y)<-[:teacherOf]-(z) RETURN ID(x), ID(y), ID(z)");
        index = experiments.index(4, 1136874830, null);
        assert(query == index);

        query = experiments.query("MATCH (x)-[:memberOf]->(y)<-[:subOrganizationOf]-(z) RETURN ID(x), ID(y), ID(z)");
        index = experiments.index(4, 1491269145, null);
        assert(query == index);
*/
        query = experiments.query("MATCH (x)-[:memberOf]->(y)-[:subOrganizationOf]->(z) RETURN ID(x), ID(y), ID(z)");
        //index = experiments.index(4, 90603815, null);
        //index = experiments.index(4, (649439727 + 1190990026), null);
        long pathId = 1235460551l + 1918060825l;
        index = experiments.index(4, pathId, null);
        assert(query == index);
/*
        query = experiments.query("MATCH (x)-[:undergraduateDegreeFrom]->(y)<-[:subOrganizationOf]-(z)<-[:memberOf]-(x) RETURN ID(x), ID(y), ID(z)");
        index = experiments.index(4, 1947276320, null);
        assert(query == index);

        query = experiments.query("MATCH (x)-[:hasAdvisor]->(y)-[:teacherOf]->(z)<-[:takesCourse]-(x) RETURN ID(x), ID(y), ID(z)");
        index = experiments.index(4, 1924021844, null);
        assert(query == index);

        query = experiments.query("MATCH (x)<-[:headOf]-(y)-[:worksFor]->(z)<-[:subOrganizationOf]-(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        index = experiments.index(5, 1628983526, null);
        assert(query == index);

        query = experiments.query("MATCH (x)<-[:headOf]-(y)-[:worksFor]->(z)-[:subOrganizationOf]->(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        index = experiments.index(5, 1084110810, null);
        assert(query == index);
*/

        for(DiskCache disk : experiments.disks.values()){
            disk.shutdown();
        }
    }

    public LUBMExperiments() throws IOException {


        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(CleverIndexBuilder.INDEX_METADATA_PATH)));
        String line;
        while((line = bufferedReader.readLine()) != null) {
            List<String> entry = Arrays.asList(line.split(","));
            int k = new Integer(entry.get(0));
            long root = new Long(entry.get(1));
            DiskCache disk = DiskCache.persistentDiskCache("K"+k+CleverIndexBuilder.LUBM_INDEX_PATH);
            indexes.put(k, new NodeTree(root, disk));
            disks.put(k, disk);
        }
        bufferedReader.close();


        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(CleverIndexBuilder.DB_PATH).newGraphDatabase();
        ggo = GlobalGraphOperations.at(database);
    }

    public int query(String cypher){
        int count = 0;
        try(Transaction tx = database.beginTx()){
            for(RelationshipType each : ggo.getAllRelationshipTypes()){
                //System.out.println(each);
            }
            System.out.println("\n" + cypher);
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
            System.out.println("Number of results found in Neo4j:" + count);
            System.out.print("Neo4j: Time to first result(ms): " + (timeToFirstResult - startTime) / 1000000);
            System.out.println(", Time to last result(ms): " + (timeToLastResult - startTime) / 1000000);
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
        //from last run: memberOfSubOrganizationOf = 90603815
        //takesCourseteacherOf --> : 1050811698
        //System.out.println("Index Searching Path: " + pathIDBuilder.path.toString());
        //System.out.println("Path ID searching for: " + pathID);
        long[] searchKey = new long[]{pathID};
        //System.out.println(disks.get(index).pageCacheFile);

        long[] foundKey;
        int count = 0;
        SearchCursor searchCursor = indexes.get(index).find(searchKey);
        try (PageProxyCursor cursor = disks.get(index).getCursor(searchCursor.pageID, PagedFile.PF_EXCLUSIVE_LOCK)) {
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
        System.out.print("Path Index: Time to first result(ms): " + (timeToFirstResult - startTime) / 1000000);
        System.out.println(", Time to last result(ms): " + (timeToLastResult - startTime) / 1000000);
        System.out.println("Result Set Size: " + count);

        if(tx != null){
            tx.close();
        }
    return count;
    }
}

