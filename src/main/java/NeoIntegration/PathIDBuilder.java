package NeoIntegration;


import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class PathIDBuilder {
    StringBuilder path = new StringBuilder();

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
        }
        else{
            path.append(new StringBuffer(relA.name()).reverse());
        }
        if(relBOutgoing){
            path.append(relB.name());
        }
        else{
            path.append(new StringBuilder(relB.name()).reverse());
        }
    }

    public PathIDBuilder addRelationship(Node node, Relationship relationship){
        if(isOutgoing(node, relationship)){
            path.append(relationship.getType().name());
        }
        else{
            path.append(new StringBuilder(relationship.getType().name()).reverse());
        }
        return this;
    }
    public long buildPath(){
        return Math.abs(path.toString().hashCode());
    }

    public boolean isOutgoing(Node node, Relationship rel){
        return rel.getStartNode().getId() == node.getId();
    }
}
