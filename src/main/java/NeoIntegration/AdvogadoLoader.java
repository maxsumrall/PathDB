/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package NeoIntegration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class AdvogadoLoader {
    public static String filePath = "out.advogato";
    public static final String DB_PATH = "graph.db/";
    public static BatchInserter inserter;
    public static HashMap<Integer, RelationshipType> relationships = new HashMap<>();


    public static void main(String[] args) throws IOException {
        AdvogadoLoader loader = new AdvogadoLoader();
        loader.run();
        inserter.shutdown();
    }

    public AdvogadoLoader() throws IOException {
        File deleteIndex = new File(DB_PATH);
        FileUtils.deleteRecursively(deleteIndex);

        inserter = BatchInserters.inserter(DB_PATH);
        relationships.put(1, (RelationshipType) DynamicRelationshipType.withName("master"));
        relationships.put(6, (RelationshipType) DynamicRelationshipType.withName("apprentice"));
        relationships.put(8, (RelationshipType) DynamicRelationshipType.withName("journeyer"));

    }

    public void run(){

        BufferedReader bufferedReader = null;
        String line;
        try {
            bufferedReader = new BufferedReader(new FileReader(filePath));
            while((line = bufferedReader.readLine()) != null) {
                String[] row = line.split(" ");
                insert(new Long(row[0]), new Long(row[1]), new Integer(row[2].replace(".","")));
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void insert(long start, long to, int edge){
        createNodeIfNeeded(start);
        createNodeIfNeeded(to);
        inserter.createRelationship(start, to, relationships.get(edge), null);
    }

    private void createNodeIfNeeded(long nodeId){
        if(!inserter.nodeExists(nodeId)){
            inserter.createNode(nodeId, null);
        }
    }
}
