/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package NeoIntegration;


import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class PathIDBuilder {
    public StringBuilder path = new StringBuilder();
    public StringBuilder prettyPrint = new StringBuilder();
    public long pathID = 0;
    public long inverse = 0;
    public PathIDBuilder(Node node1, Relationship relationship1, Node node2){
        addRelationship(node1, relationship1);
    }

    public PathIDBuilder(Node node1, Relationship relationship1, Node node2, Relationship relationship2, Node node3){
        addRelationship(node1, relationship1);
        addRelationship(node2, relationship2);
    }

    public PathIDBuilder(Node node1, Relationship relationship1, Node node2, Relationship relationship2, Node node3, Relationship relationship3, Node node4){
        addRelationship(node1, relationship1);
        addRelationship(node2, relationship2);
        addRelationship(node3, relationship3);
    }

    public PathIDBuilder(String relA, String relB){
        path.append(relA);
        path.append(relB);
    }

    public PathIDBuilder(Node nodeA, Node nodeB, Relationship relA, Relationship relB){
        addRelationship(nodeA, relA);
        addRelationship(nodeB, relB);
    }
    public PathIDBuilder(Node nodeA, Node nodeB, Node nodeC, Relationship relA, Relationship relB, Relationship relC){
        addRelationship(nodeA, relA);
        addRelationship(nodeB, relB);
        addRelationship(nodeC, relC);
    }
    public PathIDBuilder(RelationshipType relA, RelationshipType relB, boolean relAOutgoing, boolean relBOutgoing){
        if(relAOutgoing){
            path.append(relA.name());
            prettyPrint.append("()-[:" + relA.name() + "]->");
        }
        else{
            path.append(new StringBuffer(relA.name()).reverse());
            prettyPrint.append("()<-[:"+relA.name() +"]-");

        }
        if(relBOutgoing){
            path.append(relB.name());
            prettyPrint.append("()-[:"+relB.name()+"]->()");

        }
        else{
            path.append(new StringBuilder(relB.name()).reverse());
            prettyPrint.append("()<-[:"+relB.name()+"]-()");

        }
    }

    public PathIDBuilder addRelationship(Node node, Relationship relationship){
        if(isOutgoing(node, relationship)){
            path.append(relationship.getType().name());
            prettyPrint.append("()-[:"+relationship.getType().name()+"]->()");

        }
        else{
            path.append(new StringBuilder(relationship.getType().name()).reverse());
            prettyPrint.append("()<-[:"+relationship.getType().name()+"]-()");

        }
        return this;
    }
    public long buildPath(){
        if(pathID == 0l){
            pathID = Math.abs(path.toString().hashCode());
        }
        return pathID;
    }
    public String getPath(){
        return path.toString();
    }

    public long buildInversePathID(){
        if(inverse == 0l){
            inverse = Math.abs((path.reverse()).toString().hashCode());
        }
        return inverse;    }

    public boolean isOutgoing(Node node, Relationship rel){
        return rel.getStartNode().getId() == node.getId();
    }

    public String toString(){
        //return path + ":" + buildPath();
        return prettyPrint() + " " + buildPath();
    }
    public String prettyPrint(){
        return prettyPrint.toString().replace("()()", "()");
    }


    public static boolean lexicographicallyFirst(PathIDBuilder a, PathIDBuilder b){
        StringBuilder normal = new StringBuilder();
        normal.append(a.getPath());
        normal.append(b.getPath());
        String normStr = normal.toString();
        String inverse = normal.reverse().toString();
        return normStr.compareTo(inverse) < 0;
    }

}
