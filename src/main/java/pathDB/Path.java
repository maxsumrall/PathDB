package pathDB;

import java.util.List;

public class Path
{
    final int length;
    private List<Node> nodes;
    private List<Edge> edges;

    public Path( int length, List<Node> nodes, List<Edge> edges )
    {
        this.length = length;
        this.nodes = nodes;
        this.edges = edges;
    }
}
