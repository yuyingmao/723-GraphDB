package ComputeDMP;

import java.io.File;
import java.util.*;

import javafx.util.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class ComputeDMP {
	private static Map<Long,Pair<Integer,Integer>> sortByCoreNumberDesc(Map<Long, Pair<Integer,Integer>> hm){
		// Create a list from elements of HashMap
		List<Map.Entry<Long, Pair<Integer,Integer>> > list = new LinkedList<>(hm.entrySet());

		// Sort the list
		list.sort(Comparator.comparingInt((Map.Entry<Long, Pair<Integer, Integer>> o) -> o.getValue().getKey())
				.thenComparingInt(o -> o.getValue().getValue()));

		// put data from sorted list to hashmap reversely
		HashMap<Long, Pair<Integer,Integer>> temp = new LinkedHashMap<>();
		for(int i=list.size();i>0;i--){
			Map.Entry<Long, Pair<Integer,Integer>> aa=list.get(i-1);
			temp.put(aa.getKey(), aa.getValue());
		}
		return temp;
	}

	private static int comparePairs(Pair<Integer,Integer> p1,Pair<Integer,Integer> p2){
		int result= p1.getKey()-p2.getKey();
		if(result==0)
			result=p1.getValue()-p2.getValue();
		return result;
	}

	private static Map<Long,Integer> sortByDegreeDesc(Map<Long,Integer> hm){
		// Create a list from elements of HashMap
		List<Map.Entry<Long, Integer> > list = new LinkedList<>(hm.entrySet());

		// Sort the list
		list.sort(Map.Entry.comparingByValue());

		// put data from sorted list to hashmap
		HashMap<Long, Integer> temp = new LinkedHashMap<>();
		for(int i=list.size();i>0;i--){
			Map.Entry<Long, Integer> aa=list.get(i-1);
			temp.put(aa.getKey(), aa.getValue());
		}
		return temp;
	}

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];

		System.out.println(new Date() + " -- Started");
		
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolder))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M").newGraphDatabase();
		
		// TODO: Your code here!!!!
		// A node can have either its psi or psiest property set, use COALESCE to select the one that is set.
		// Recall that you must compute realistic ranks, so you must keep track of all the nodes that have repeated values to provide their ranks.
		// You must set properties rankc and rankd for each node.

		//nodesRankC=Map<nodeID,<coreNumber,degree>>
		Map<Long,Pair<Integer,Integer>> nodesRankC=new HashMap<>();
		try(Result rs=db.execute("MATCH (v) WITH v, size((v)--()) as degree RETURN ID(v), degree, COALESCE(v.psi,v.psiest)")){
			while (rs.hasNext()){
				Map<String, Object> row=rs.next();
				Pair<Integer,Integer> coreNumberAndDegree=new Pair<>(((Number)row.get("COALESCE(v.psi,v.psiest)")).intValue(),
						((Number)row.get("degree")).intValue());
				nodesRankC.put((long)row.get("ID(v)"),coreNumberAndDegree);
			}
		}
		nodesRankC=sortByCoreNumberDesc(nodesRankC);
		int currentRank=1;
		//rankc computation
		Iterator<Map.Entry<Long,Pair<Integer,Integer>>> iteratorC=nodesRankC.entrySet().iterator();
		Map.Entry<Long,Pair<Integer,Integer>> currentNodeC=iteratorC.next(); //first node
		Map.Entry<Long,Pair<Integer,Integer>> prevNodeC=null;
		while(prevNodeC!=currentNodeC){
			int startRank=currentRank;
			int endRank=startRank;
			prevNodeC=currentNodeC;
			//find node(s) that has same core number and degree
			ArrayList<Long> nodesWithSameRank=new ArrayList<>();
			nodesWithSameRank.add(currentNodeC.getKey());
			while(iteratorC.hasNext()){
				Map.Entry<Long,Pair<Integer,Integer>> nextNode=iteratorC.next();
				if(comparePairs(currentNodeC.getValue(),nextNode.getValue())==0){
					nodesWithSameRank.add(nextNode.getKey());
					endRank++;
				}
				else{
					currentNodeC=nextNode;
					break;
				}
			}
			//calculate realistic rank
			double realisticRank=(double)(startRank+endRank)/2;
			//update these nodes
			Transaction tx=db.beginTx();
			for(long updateNode:nodesWithSameRank){
				db.getNodeById(updateNode).setProperty("rankc",realisticRank);
				tx.success();
			}
			tx.close();

			currentRank=endRank+1;
		}

		//copy nodeID and degree from nodesRankC
		// nodesRankD=Map<nodeID,degree>
		Map<Long,Integer> nodesRankD=new HashMap<>();
		for(long node:nodesRankC.keySet())
			nodesRankD.put(node,nodesRankC.get(node).getValue());
		nodesRankD=sortByDegreeDesc(nodesRankD);
		//rankd computation
		currentRank=1;
		Iterator<Map.Entry<Long,Integer>>iteratorD=nodesRankD.entrySet().iterator();
		Map.Entry<Long,Integer> currentNodeD=iteratorD.next();
		Map.Entry<Long,Integer> prevNodeD=null;
		while(prevNodeD!=currentNodeD){
			int startRank=currentRank;
			int endRank=startRank;
			prevNodeD=currentNodeD;
			//find node(s) that has same core number and degree
			ArrayList<Long> nodesWithSameRank=new ArrayList<>();
			nodesWithSameRank.add(currentNodeD.getKey());
			while(iteratorD.hasNext()){
				Map.Entry<Long,Integer> nextNode=iteratorD.next();
				if(currentNodeD.getValue().equals(nextNode.getValue())){
					nodesWithSameRank.add(nextNode.getKey());
					endRank++;
				}
				else{
					currentNodeD=nextNode;
					break;
				}
			}
			//calculate realistic rank
			double realisticRank=(double)(startRank+endRank)/2;
			//update these nodes
			Transaction tx=db.beginTx();
			for(long updateNode:nodesWithSameRank){
				db.getNodeById(updateNode).setProperty("rankd",realisticRank);
				tx.success();
			}
			tx.close();

			currentRank=endRank+1;
		}

		// TODO: End of your code.
		
		db.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}

}
