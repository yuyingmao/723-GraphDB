package CoreNumberUpperBoundEstimation;

import java.io.File;
import java.util.*;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class CoreNumberUpperBoundEstimation {

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

	public static void main(String[] args) {
		final String neo4jFolder = args[0];
		final int ki = Integer.parseInt(args[1]), ke = Integer.parseInt(args[2]);

		System.out.println(new Date() + " -- Started");
		
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolder))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M").newGraphDatabase();

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
		db.shutdown();

		System.out.println(new Date() + " -- Done");
	}

}
