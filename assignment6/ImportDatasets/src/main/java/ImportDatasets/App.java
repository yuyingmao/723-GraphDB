package ImportDatasets;

import javafx.util.Pair;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.util.*;

public class App {
    private static void createNodes(Map<Long,String> entities, BatchInserter inserter){
        for(long nID:entities.keySet()){
            Map<String,Object> property=new HashMap<>();
            String entity=entities.get(nID);
            if(entity!=null)
                property.put("entity",entity);
            inserter.createNode(nID,property);
        }
    }
    private static void createRelations(Map<Long,Set<Pair<Long,Integer>>> triples, Map<Integer,String> relationTypes, BatchInserter inserter){
        for(long sID:triples.keySet()){
            Set<Pair<Long,Integer>> neighbors=triples.get(sID);
            for(Pair<Long,Integer> neighbor:neighbors){
                long oID=neighbor.getKey();
                int pID=neighbor.getValue();
                Map<String,Object> pProperty=new HashMap<>();
                String relationType=relationTypes.get(pID);
                if(relationType!=null)
                    inserter.createRelationship(sID,oID,RelationshipType.withName(relationType),pProperty);
                else
                    inserter.createRelationship(sID,oID,RelationshipType.withName("null"),pProperty);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        File folder=new File("../KGC_datasets");
        if(folder.isDirectory()){
            File[] listOfDatasets=folder.listFiles();
            assert listOfDatasets != null;
            for(File dataset:listOfDatasets){
                if(dataset.isDirectory()){
                    File[] files=dataset.listFiles();
                    assert files != null;
                    Map<Long,Set<Pair<Long,Integer>>> triples=new HashMap<>();
                    Map<Long,String> entities=new HashMap<>();
                    Map<Integer,String> relationTypes=new HashMap<>();
                    for(File file:files){
                        Scanner scanner=new Scanner(file);
                        if(file.getName().equals("dataset.txt")){
                            while(scanner.hasNextLine()){
                                String[] rs=scanner.nextLine().split("\\s+");
                                long s=Long.parseLong(rs[0]);
                                long o=Long.parseLong(rs[1]);
                                int p=Integer.parseInt(rs[2]);
                                entities.put(s,null);
                                entities.put(o,null);
                                if(triples.containsKey(s))
                                    triples.get(s).add(new Pair<>(o,p));
                                else{
                                    Set<Pair<Long,Integer>> neighbors=new HashSet<>();
                                    neighbors.add(new Pair<>(o,p));
                                    triples.put(s,neighbors);
                                }
                            }
                        }
                        else if(file.getName().equals("entity2id.txt")){
                            while(scanner.hasNextLine()){
                                String[] rs=scanner.nextLine().split("\\s+");
                                if(rs.length==2)
                                    entities.put(Long.parseLong(rs[1]),rs[0]);
                            }
                        }
                        else if(file.getName().equals("relation2id.txt")){
                            while(scanner.hasNextLine()){
                                String[] rs=scanner.nextLine().split("\\s+");
                                if(rs.length==2)
                                    relationTypes.put(Integer.parseInt(rs[1]),rs[0]);
                            }
                        }
                        else
                            System.err.println("get "+file.getName());
                        scanner.close();
                    }
                    BatchInserter inserter = BatchInserters.inserter(new File("../db/"+dataset.getName()));
                    createNodes(entities,inserter);
                    createRelations(triples,relationTypes,inserter);
                    inserter.shutdown();
                }
            }
        }

    }
}
