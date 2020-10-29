package TopDownKCore;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

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
		Transaction tx=db.beginTx();

		//a hashmap contains nodes' internal id and their 'psiest' value
		Map<Long,Integer> vs=new HashMap<>();
		//get all nodes that have a psiest between ki and ke
		try(Result rs=db.execute("MATCH (v) WHERE "+ki+"<=v.psiest<="+ke+" RETURN ID(v),v.psiest")){
			while(rs.hasNext()){
				Map<String, Object> row = rs.next();
				vs.put((long)row.get("ID(v)"),((Number)row.get("v.psiest")).intValue());
			}
		}

		//sort them in ascending order of core upper bound
		vs=sortByCoreUpperBound(vs);

		for(Long v:vs.keySet()){
			int vPsiest=vs.get(v);
			//find neighbor(s) of v which has a core upper bound less than v
			Map<Long,Integer> z=new HashMap<>();
			try(Result rs=db.execute("MATCH (v)--(u) WHERE ID(v)="+v+" RETURN ID(u),u.psiest")){
				while(rs.hasNext()){
					Map<String, Object> row = rs.next();
					long u=(long)row.get("ID(u)");
					int uPsiest=((Number)row.get("u.psiest")).intValue();
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
		}
		tx.close();
	}

	private static void generateGPremium(String neo4jFolderCopy,Map<Long,Integer> vPremium,Map<Long,Set<Long>> ePremium) throws IOException {
		BatchInserter inserter = BatchInserters.inserter(new File(neo4jFolderCopy));
		//create nodes
		for(long vID:vPremium.keySet()){
			Map<String,Object> property=new HashMap<>();
			property.put("deposit",vPremium.get(vID));
			inserter.createNode(vID,property, Label.label("Node"));
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

	private static void BottomUpKCore(GraphDatabaseService db,String neo4jFolderCopy,int ki,int ke,int numOfV){
		GraphDatabaseService dbCopy = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolderCopy))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M").newGraphDatabase();

		long total = numOfV;
		long current = ki;
		while(total>0){
			while(true){
				long totalBefore=total;
				Transaction txCopy=dbCopy.beginTx();
				int numOfNodeToDelete=0;
				try(Result rs=dbCopy.execute("MATCH (v) WITH v, size((v)--()) as degree WHERE degree<"+current+" RETURN ID(v)")){
					while (rs.hasNext()){
						numOfNodeToDelete++;
						dbCopy.execute("MATCH (n) WHERE ID(n)="+rs.next().get("ID(v)")+" DETACH DELETE n");
						txCopy.success();
					}
				}
				txCopy.close();
				total=total-numOfNodeToDelete;
				if(totalBefore==total)
					break;
			}
			Transaction txCopy=dbCopy.beginTx();
			Transaction txOrig=db.beginTx();
			try(Result rs=dbCopy.execute("MATCH (n) RETURN ID(n)")){
				while (rs.hasNext()){
					db.getNodeById((long)rs.next().get("ID(n)")).setProperty("psi",current);
					txOrig.success();
				}
			}
			txOrig.close();
			txCopy.close();
			if(current<ke)
				current++;
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
		
		int ke=((Number)db.execute("MATCH (v) WITH size((v)--()) as degree RETURN MAX(degree)").next().get("MAX(degree)")).intValue();
		int ki=Math.max(kmin,ke-step);

		do {
			CoreNumberUpperBoundEstimation(db, ki, ke);
			//generate V'=map<nodeID,deposit>
			Map<Long, Integer> vPremium = new HashMap<>();
			try (Result rs = db.execute("MATCH (v) WHERE " + ki + "<=v.psiest<=" + ke + " RETURN ID(v),v.deposit")) {
				while (rs.hasNext()) {
					Map<String, Object> row = rs.next();
					vPremium.put((long)row.get("ID(v)"),((Number)row.get("v.deposit")).intValue());
				}
			}
			if (vPremium.size() >= ki) {
				//generate E'=Map<nodeID,neighbors>
				Map<Long,Set<Long>> ePremium=new HashMap<>();
				for(long vID:vPremium.keySet()){
					try (Result rs = db.execute("MATCH (v)--(u) WHERE ID(v)="+vID+ " RETURN ID(u)")) {
						while (rs.hasNext()) {
							long uID = (long) rs.next().get("ID(u)");
							if(vPremium.containsKey(uID) && vID<uID){
								if(!ePremium.containsKey(vID)){
									Set<Long> neighbors=new HashSet<>();
									neighbors.add(uID);
									ePremium.put(vID,neighbors);
								}
								else
									ePremium.get(vID).add(uID);
							}
						}
					}
				}
				//generate G'=(V',E') and copy the deposits
				generateGPremium(neo4jFolderCopy,vPremium,ePremium);
				BottomUpKCore(db,neo4jFolderCopy,ki,ke,vPremium.size());
			}


			Transaction tx=db.beginTx();
			for(long vID:vPremium.keySet()) {
				Node v=db.getNodeById(vID);
				if(v.hasProperty("psi")){
					//find N(v)
					Set<Long> neighbors=new HashSet<>();
					try (Result rs = db.execute("MATCH (v)--(u) WHERE ID(v)="+vID+ " RETURN ID(u)")) {
						while (rs.hasNext()) {
							neighbors.add((long) rs.next().get("ID(u)"));
						}
					}
					//update neighbors' deposit
					for(long uID:neighbors){
						Node u=db.getNodeById(uID);
						if(!u.hasProperty("psi")){
							int uDeposit = ((Number) u.getProperty("deposit")).intValue();
							u.setProperty("deposit", uDeposit + 1);
							tx.success();
						}
					}
				}
				else{
					v.setProperty("psiest",ki-1);
					tx.success();
				}
			}
			tx.close();
			ke = ki - 1;
			ki = Math.max(kmin, ke - step);
		} while(ke-ki>=0);

		try (Result rs = db.execute("MATCH (v) WHERE EXISTS(v.psi) RETURN ID(v),v.psi")) {
			while (rs.hasNext()) {
				Map<String, Object> row = rs.next();
				for ( String key : rs.columns() )
					System.out.printf( "%s = %s%n", key, row.get( key ) );
			}
		}
		
		db.shutdown();

		System.out.println(new Date() + " -- Done");
	}
	
}
