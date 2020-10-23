package TopDownKCore;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import scala.Int;

public class TopDownKCore {

	private static Map<Long,Integer> sortByCoreUpperBound(Map<Long,Integer> hm){
		// Create a list from elements of HashMap
		List<Map.Entry<Long, Integer> > list = new LinkedList<>(hm.entrySet());

		// Sort the list
		list.sort(Map.Entry.comparingByValue());

		// put data from sorted list to hashmap
		HashMap<Long, Integer> temp = new LinkedHashMap<>();
		for (Map.Entry<Long, Integer> aa : list) {
			temp.put(aa.getKey(), aa.getValue());
		}
		return temp;
	}

	private static void CoreNumberUpperBoundEstimation(GraphDatabaseService db,int ki,int ke){
		Map<Long,Integer> vs=new HashMap<>();
		//get all nodes that have a psiest between ki and ke
		Transaction tx=db.beginTx();
		try(Result rs=db.execute("MATCH (n) WHERE "+ki+"<=n.psiest<="+ke+" RETURN ID(n),n.psiest")){
			while(rs.hasNext()){
				Map<String, Object> row = rs.next();
				vs.put(Long.parseLong(String.valueOf(row.get("ID(n)"))),
						Integer.parseInt(String.valueOf(row.get("n.psiest"))));
			}
		}
		tx.close();

		//sort them in ascending order of core upper bound
		vs=sortByCoreUpperBound(vs);

		for(Long v:vs.keySet()){
			tx= db.beginTx();
			int vPsiest=vs.get(v);
			//find neighbor(s) of v which has a core upper bound less than v
			Map<Long,Integer> z=new HashMap<>();
			try(Result rs=db.execute("MATCH (v)--(m) WHERE ID(v)="+v+" RETURN ID(m),m.psiest")){
				while(rs.hasNext()){
					Map<String, Object> row = rs.next();
					long u=Long.parseLong(String.valueOf(row.get("ID(m)")));
					int uPsiest=Integer.parseInt(String.valueOf(row.get("m.psiest")));
					if(uPsiest<vPsiest)
						z.put(u,uPsiest);
				}
			}
			Node vNode=db.getNodeById(v);
			int vDegree=vNode.getDegree();
			//if |N(v)|-|Z(v)|<v.psiest
			if(vDegree-z.size()<vPsiest){
				z=sortByCoreUpperBound(z);
				int i=0;
				for(Long u: z.keySet()){
					int f=Math.max(vDegree-(i+1),z.get(u));
					if(f<vPsiest)
						vPsiest=f;
					i+=1;
				}
			}
			//update 'psiest' property
			vNode.setProperty("psiest",vPsiest);
			tx.success();
			tx.close();
		}
	}

	private static void generateGPremium(String neo4jFolderCopy,Map<Long, Integer> vPremium,
										 Map<Long,Set<Long>> ePremium) throws IOException {
		BatchInserter inserter = BatchInserters.inserter(new File(neo4jFolderCopy));
		//create nodes
		for(long vID:vPremium.keySet()){
			Map<String,Object> property=new HashMap<>();
			property.put("deposit",vPremium.get(vID));
			inserter.createNode(vID,property,Label.label("Node"));
		}

		//create relationships
		for(long v:ePremium.keySet()){
			Set<Long> neighbors=ePremium.get(v);
			for(long u:neighbors){
				Map<String,Object> property=new HashMap<>();
				inserter.createRelationship(v,u,RelationshipType.withName("Relation"),property);
			}
		}
		inserter.shutdown();
	}

	private static void BottomUpKCore(GraphDatabaseService db,String neo4jFolderCopy,int ki,int ke,int totalVPremium) throws IOException {
		GraphDatabaseService dbCopy = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolderCopy))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M").newGraphDatabase();
		long total=totalVPremium;

		long current = ki;
		long totalBefore;

		Transaction txOrig,txCopy;
		while(total > 0){
			do {
				totalBefore = total;
				try(Result nodesToDelete = dbCopy.execute("MATCH (n) WITH ID(n) AS index, size((n) -- ()) AS ct, n.deposit AS p WHERE ct+p < " + current + " RETURN index")){
					while (nodesToDelete.hasNext()){
						txOrig=db.beginTx();
						long node = (long)nodesToDelete.next().get("index");
						total-=1;
						db.getNodeById(node).setProperty("psi",current-1);
						txOrig.success();
						txOrig.close();
					}

				}
				txCopy = dbCopy.beginTx();
				dbCopy.execute("MATCH (n) WITH n AS node, size((n) -- ()) AS ct WHERE ct+node.deposit < " + current + " DETACH DELETE node");
				txCopy.success();
				txCopy.close();
			} while (total != totalBefore);
			if(current<ke)
				current+=1;
		}
		dbCopy.shutdown();
	}

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0],
				neo4jFolderCopy = args[1];
		final int kmin = Integer.parseInt(args[2]),
				step = Integer.parseInt(args[3]);

		System.out.println(new Date() + " -- Started");
		
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolder))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M").newGraphDatabase();
		
		// TODO: Your code here!!!!
		// At the beginning, each node will have psi=null, psiest=degree and deposit=0. In each step, you must start with the nodes whose psiest is between ki and ke,
		//		where ke is the max degree at the beginning. You must update both ke and ki in each step.
		//	The upper bound refinement is generally the bottleneck of the algorithm; limit the upper bound refinement to no more than 10,000 nodes each time.
		//	The auxiliary folder must be removed in every step and, if there are enough nodes, use it to copy the relevant subgraph and compute the core numbers in a 
		//		bottom-up fashion. Note that you must copy the deposits of the nodes as well.
		//	When you set the psi property of a node, you should remove its psiest property.

		Transaction dbTX=db.beginTx();
		//get ke, the maximum degree in G
		int ke=((Number)db.execute("MATCH (n) RETURN MAX(n.psiest)").next().get("MAX(n.psiest)")).intValue();
		dbTX.close();
		int ki=Math.max(ke-step,kmin);
		do {
			CoreNumberUpperBoundEstimation(db, ki, ke);
			//generate V'
			dbTX = db.beginTx();
			Map<Long, Integer> vPremium = new HashMap<>();
			try (Result rs = db.execute("MATCH (v) WHERE " + ki + "<=v.psiest<=" + ke + " RETURN ID(v),v.deposit")) {
				while (rs.hasNext()) {
					Map<String, Object> row = rs.next();
					vPremium.put((long)row.get("ID(v)"),((Number)row.get("v.deposit")).intValue());
				}

			}
			if (vPremium.size() >= ki) {
				//generate E'
				Map<Long, Set<Long>> ePremium = new HashMap<>();
				for (Long vID : vPremium.keySet()) {
					try (Result rs = db.execute("MATCH (v)--(u) WHERE ID(v)=" + vID + " RETURN ID(u)")) {
						while (rs.hasNext()) {
							long uID = (long) rs.next().get("ID(u)");
							if (vPremium.containsKey(uID) && vID<uID) {
								if (ePremium.containsKey(vID))
									ePremium.get(vID).add(uID);
								else {
									Set<Long> neighbors = new HashSet<>();
									neighbors.add(uID);
									ePremium.put(vID, neighbors);
								}
							}
						}
					}
				}
				dbTX.close();
				//generate G'=(V',E') and copy the deposits
				generateGPremium(neo4jFolderCopy, vPremium, ePremium);

				BottomUpKCore(db, neo4jFolderCopy, ki, ke, vPremium.size());
			}

			for (long vID : vPremium.keySet()) {
				dbTX = db.beginTx();
				Node v = db.getNodeById(vID);
				if (v.hasProperty("psi")) {
					//get N(v)
					Set<Long> neighbors = new HashSet<>();
					try (Result rs = db.execute("MATCH (v)--(u) WHERE ID(v)=" + vID + " RETURN ID(u)")) {
						while (rs.hasNext())
							neighbors.add((long)rs.next().get("ID(u)"));
					}
					//update deposit
					for (long uID : neighbors) {
						Node u = db.getNodeById(uID);
						if (!u.hasProperty("psi")) {
							int uDeposit = ((Number) u.getProperty("deposit")).intValue();
							u.setProperty("deposit", uDeposit + 1);
						}
					}
				} else
					v.setProperty("psiest", ki - 1);
				dbTX.success();
				dbTX.close();
			}

			ke = ki - 1;
			ki = Math.max(kmin, ke - step);
		} while (ke - ki >= 0);
		// TODO: End of your code.
		try (Result rs = db.execute("MATCH (v) WHERE EXISTS(v.psi) RETURN ID(v),v.psi")) {
			while (rs.hasNext()) {
				Map<String, Object> row = rs.next();
				for ( String key : rs.columns() )
				{
					System.out.printf( "%s = %s%n", key, row.get( key ) );
				}
			}

		}
		db.shutdown();

		System.out.println(new Date() + " -- Done");
	}
	
}
