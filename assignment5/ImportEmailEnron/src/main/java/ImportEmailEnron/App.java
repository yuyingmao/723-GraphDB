package ImportEmailEnron;

import java.io.*;
import java.util.*;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class App {

    public static void main(String[] args){
        //a hashmap used to store all the nodes and their corresponding neighbors
        Map<Long, Set<Long>> nodes=new HashMap<>();

        //read all nodes and their neighbors from the text file
        try {
            Scanner sc = new Scanner(new FileInputStream("../Email-Enron.txt"));
            while(sc.hasNextLine()){
                String[] rs=sc.nextLine().split("\t");
                long n1=Long.parseLong(rs[0]);
                long n2=Long.parseLong(rs[1]);
                if(nodes.containsKey(n1))
                    nodes.get(n1).add(n2);
                else{
                    Set<Long> neighbors=new HashSet<>();
                    neighbors.add(n2);
                    nodes.put(n1,neighbors);
                }
            }
            sc.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            BatchInserter inserter = BatchInserters.inserter(new File("../db/Email-Enron"));
            //create nodes, each node has structure:
            // internal id=node id; label="Node"
            for(Long nodeID:nodes.keySet()){
                Map<String,Object> property=new HashMap<>();
                inserter.createNode(nodeID,property,Label.label("Node"));
            }
            //create relationships, each relationship has structure:
            // label="Relation"; no property
            for(Long nodeID:nodes.keySet()){
                Set<Long> neighbors=nodes.get(nodeID);
                for(Long neighborID:neighbors){
                    if(nodeID<neighborID){
                        Map<String,Object> rProperty=new HashMap<>();
                        inserter.createRelationship(nodeID,neighborID,RelationshipType.withName("Relation"),rProperty);
                    }
                }
            }
            inserter.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
