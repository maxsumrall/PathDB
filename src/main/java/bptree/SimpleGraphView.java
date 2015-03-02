package bptree; /**
 * Created by max on 2/19/15.
 */
/*
 * bptree.SimpleGraphView.java
 *
 * Created on March 8, 2007, 7:49 PM; Updated May 29, 2007
 *
 * Copyright March 8, 2007 Grotto Networking
 */

import edu.uci.ics.jung.algorithms.layout.CircleLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DelegateTree;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.SparseMultigraph;
import edu.uci.ics.jung.graph.Tree;
import edu.uci.ics.jung.visualization.BasicVisualizationServer;
import java.awt.Dimension;
import java.io.FileNotFoundException;
import java.util.Random;
import javax.swing.JFrame;

/**
 *
 * @author Dr. Greg M. Bernstein
 */
public class SimpleGraphView {
    DelegateTree<Integer, String> g;
    /*
    Graph Details
     */


    BPTree db;
    Random rnd = new Random();
    int numberOfKeys = 10;


    /** Creates a new instance of bptree.SimpleGraphView */
    public SimpleGraphView() throws FileNotFoundException {
        // Graph<V, E> where V is the type of the vertices and E is the type of the edges
        g = new DelegateTree<Integer, String>();

        //Insert things into BTree
        db = new BPTree();
        for (int i = 0; i < numberOfKeys; i++) {
            db.insert(new Key(new long[]{i, i, i}));
        }
        BuildGraph(g, db.bm.rootBlock);

    }

    public static void BuildGraph(DelegateTree g, Block rootBlock){

        g.addVertex(rootBlock.blockID);
        addNodeAndEdges(g, rootBlock);
    }

    public static void addNodeAndEdges(DelegateTree g, Block block){
        if(block instanceof IBlock) {
            for (long childID : ((IBlock)block).children) {
                if(childID == 0){continue;}
                System.out.println("" + block.blockID + " ->" + childID);
                g.addChild("" + block.blockID + " ->" + childID, block.blockID, childID);
                addNodeAndEdges(g, block.blockManagerInstance.getBlock(childID));
            }
        }
    }


    public static void main(String[] args) throws FileNotFoundException {
        SimpleGraphView sgv = new SimpleGraphView(); //We create our graph in here
        // The Layout<V, E> is parameterized by the vertex and edge types
        Layout<Integer, String> layout = new CircleLayout(sgv.g);
        layout.setSize(new Dimension(300,300)); // sets the initial size of the layout space
        // The BasicVisualizationServer<V,E> is parameterized by the vertex and edge types
        BasicVisualizationServer<Integer,String> vv = new BasicVisualizationServer<Integer,String>(layout);
        vv.setPreferredSize(new Dimension(350,350)); //Sets the viewing area size

        JFrame frame = new JFrame("Simple Graph View");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(vv);
        frame.pack();
        frame.setVisible(true);
    }



}