package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.NodeTree;
import bptree.impl.SearchCursor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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

        experiments.query("MATCH (x)-[:memberOf]->(y) RETURN ID(x), ID(y)");
        experiments.index(3, 649439727);

        experiments.query("MATCH (x)-[:worksFor]->(y) RETURN ID(x), ID(y)");
        experiments.index(3, 35729895);

        experiments.query("MATCH (x)-[:takesCourse]->(y)<-[:teacherOf]-(z) RETURN ID(x), ID(y), ID(z)");
        experiments.index(4, 1136874830);

        experiments.query("MATCH (x)-[:memberOf]->(y)<-[:subOrganizationOf]-(z) RETURN ID(x), ID(y), ID(z)");
        experiments.index(4, 1491269145);

        experiments.query("MATCH (x)-[:memberOf]->(y)-[:subOrganizationOf]->(z) RETURN ID(x), ID(y), ID(z)");
        experiments.index(4, 90603815);

        experiments.query("MATCH (x)-[:undergraduateDegreeFrom]->(y)<-[:subOrganizationOf]-(z)<-[:memberOf]-(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        experiments.index(4, 1947276320);

        experiments.query("MATCH (x)-[:hasAdvisor]->(y)-[:teacherOf]->(z)<-[:takesCourse]-(x) RETURN ID(x), ID(y), ID(z)");
        experiments.index(4, 1924021844);

        experiments.query("MATCH (x)<-[:headOf]-(y)-[:worksFor]->(z)<-[:subOrganizationOf]-(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        experiments.index(5, 1628983526);

        experiments.query("MATCH (x)<-[:headOf]-(y)-[:worksFor]->(z)-[:subOrganizationOf]->(w) RETURN ID(x), ID(y), ID(z), ID(w)");
        experiments.index(5, 1084110810);


        for(DiskCache disk : experiments.disks.values()){
            disk.shutdown();
        }
    }

    public LUBMExperiments() throws IOException {


        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(BulkLUBMDataLoader.INDEX_METADATA_PATH)));
        String line;
        while((line = bufferedReader.readLine()) != null) {
            List<String> entry = Arrays.asList(line.split(","));
            int k = new Integer(entry.get(0));
            long root = new Long(entry.get(1));
            DiskCache disk = DiskCache.persistentDiskCache("K"+k+BulkLUBMDataLoader.LUBM_INDEX_PATH);
            indexes.put(k, new NodeTree(root, disk));
            disks.put(k, disk);
        }
        bufferedReader.close();


        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(BulkLUBMDataLoader.DB_PATH).newGraphDatabase();
        ggo = GlobalGraphOperations.at(database);
    }

    public void query(String cypher){
        try(Transaction tx = database.beginTx()){
            for(RelationshipType each : ggo.getAllRelationshipTypes()){
                //System.out.println(each);
            }
            System.out.println("\n" + cypher);
            System.out.println("Begin Neo4j Transaction");
            long startTime = System.nanoTime();
            Result queryAResult = database.execute(cypher);
            queryAResult.next();
            long timeToFirstResult = System.nanoTime();
            //System.out.println(queryAResult.getQueryStatistics());
            //System.out.println(queryAResult.getExecutionPlanDescription());

            int count = 0;

            while(queryAResult.hasNext()){
                //System.out.println(queryAResult.next().toString());
                queryAResult.next();
                count++;
            }
            long timeToLastResult = System.nanoTime();
            System.out.println("Number of results found in Neo4j:" + count);
            System.out.print("Neo4j Time to first result(ms): " + (timeToFirstResult - startTime) / 1000000);
            System.out.println(", Neo4j Time to last result(ms): " + (timeToLastResult - startTime) / 1000000);
        }
    }

    public void index(int index, long pathID) throws IOException {
        long startTime = System.nanoTime();
        long timeToFirstResult;
        long timeToLastResult;
        //from last run: memberOfSubOrganizationOf = 90603815
        //takesCourseteacherOf --> : 1050811698
        //System.out.println("Index Searching Path: " + pathIDBuilder.path.toString());
        System.out.println("Path ID searching for: " + pathID);
        long[] searchKey = new long[]{pathID};
        System.out.println(disks.get(index).cache_file);

        long[] foundKey;
        int count = 1;
        SearchCursor searchCursor = indexes.get(index).find(searchKey);
        try (PageProxyCursor cursor = disks.get(index).getCursor(searchCursor.pageID, PagedFile.PF_EXCLUSIVE_LOCK)) {
            searchCursor.next(cursor);
            timeToFirstResult = System.nanoTime();
            while(searchCursor.hasNext(cursor)) {
                foundKey = searchCursor.next(cursor);
                int l = foundKey.length; //todo so that the compiler doesnt wipe out the prev call. dunno.
                count++;
            }
            timeToLastResult = System.nanoTime();
        }
        System.out.println("Number of results found in Index: " + count);
        System.out.print("Index Time to first result(ms): " + (timeToFirstResult - startTime) / 1000000);
        System.out.println(", Index Time to last result(ms): " + (timeToLastResult - startTime) / 1000000);

    }




}
