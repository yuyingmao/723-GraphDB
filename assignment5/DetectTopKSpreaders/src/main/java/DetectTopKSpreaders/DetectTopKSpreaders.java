package DetectTopKSpreaders;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class DetectTopKSpreaders {

	private static Map<Long, Set<Long>> generateGC(GraphDatabaseService db,String neo4jFolderCopy){
		int maxCoreNumber=((Number)db.execute("MATCH (n) WITH COALESCE(n.psi,n.psiest) AS coreNumber RETURN MAX(coreNumber)")
				.next().get("MAX(coreNumber)")).intValue();
		Map<Long, Set<Long>> ec=new HashMap<>();
		try(Result rs=db.execute("MATCH (n) WITH ID(n) AS id, COALESCE(n.psi,n.psiest) AS coreNumber WHERE coreNumber="+maxCoreNumber+" RETURN id")){
			while (rs.hasNext())
				ec.put((long)rs.next().get("id"),new HashSet<>());
		}
		for(long vID:ec.keySet()){
			try(Result rs=db.execute("MATCH (n)--(m) WHERE ID(n)="+vID+" RETURN ID(m)")){
				while (rs.hasNext()){
					long uID=(long)rs.next().get("ID(m)");
					if(ec.containsKey(uID))
						ec.get(vID).add(uID);
				}
			}
		}
		//create nodes and relationships in Gc
		try {
			BatchInserter inserter = BatchInserters.inserter(new File(neo4jFolderCopy));
			for(long vID:ec.keySet()){
				Map<String,Object> property=new HashMap<>();
				inserter.createNode(vID,property, Label.label("Node"));
			}
			for(long vID:ec.keySet()){
				Set<Long> neighbors=ec.get(vID);
				for(long uID:neighbors){
					if(vID<uID){
						Map<String,Object> property=new HashMap<>();
						inserter.createRelationship(vID,uID, RelationshipType.withName("Relation"),property);
					}
				}
			}
			inserter.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ec;
	}

	private static void normalize(GraphDatabaseService dbCopy,Set<Long> vc){
		double s=0;
		Transaction txCopy=dbCopy.beginTx();
		for(long vID:vc)
			s=s+Math.pow((double)dbCopy.getNodeById(vID).getProperty("x"),2);
		double sqrtS=Math.sqrt(s);
		for(long vID:vc){
			double x=(double)dbCopy.getNodeById(vID).getProperty("x");
			dbCopy.getNodeById(vID).setProperty("x",x/sqrtS);
			txCopy.success();
		}
		txCopy.close();
	}

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0],
				neo4jFolderCopy = args[1];
		final int maxIter = Integer.parseInt(args[2]);
		final double epsilon = Double.parseDouble(args[3]);

		System.out.println(new Date() + " -- Started");
		
		FileUtils.deleteDirectory(new File(neo4jFolderCopy));
		
		// TODO: Your code here!!!!
		// A node can have either its psi or psiest property set, use COALESCE to select the one that is set.
		// Copy the degeneracy core from the original to the auxiliary database.
		// Compute the eigenvector centrality for each node and store its value in property x. You should also use an auxiliary property xlast.
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolder))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M").newGraphDatabase();

		Map<Long, Set<Long>> ec=generateGC(db,neo4jFolderCopy);

		GraphDatabaseService dbCopy = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolderCopy))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M").newGraphDatabase();
		int ecSize=ec.size();
		double initX=(double)1/ecSize;
		Transaction txCopy=dbCopy.beginTx();
		for(long vID:ec.keySet()){
			dbCopy.getNodeById(vID).setProperty("x",initX);
			txCopy.success();
		}
		txCopy.close();

		normalize(dbCopy,ec.keySet());

		for(int i=0;i<maxIter;i++){

			txCopy=dbCopy.beginTx();
			for(long vID:ec.keySet()){
				double x=(double)dbCopy.getNodeById(vID).getProperty("x");
				dbCopy.getNodeById(vID).setProperty("xlast",x);
				txCopy.success();
			}
			txCopy.close();

			for(long vID:ec.keySet()){
				txCopy=dbCopy.beginTx();
				double vxlast=(double)dbCopy.getNodeById(vID).getProperty("xlast");
				Set<Long> neighbors=ec.get(vID);
				for(long uID:neighbors){
					double ux=(double)dbCopy.getNodeById(uID).getProperty("x");
					dbCopy.getNodeById(uID).setProperty("x",ux+vxlast);
					txCopy.success();
				}
				txCopy.close();
			}

			normalize(dbCopy,ec.keySet());

			double e=0;
			txCopy=dbCopy.beginTx();
			for(long vID:ec.keySet()){
				double x=(double)dbCopy.getNodeById(vID).getProperty("x");
				double xlast=(double)dbCopy.getNodeById(vID).getProperty("xlast");
				e=e+Math.abs(x-xlast);
			}
			txCopy.close();
			if(e<(ecSize*epsilon))
				break;
		}

		//update x in G
		Transaction tx=db.beginTx();
		txCopy=dbCopy.beginTx();
		for(long vID:ec.keySet()){
			double x=(double)dbCopy.getNodeById(vID).getProperty("x");
			db.getNodeById(vID).setProperty("x",x);
			tx.success();
		}
		tx.close();
		txCopy.close();

		dbCopy.shutdown();
		// TODO: End of your code.
		
		db.shutdown();
		
		System.out.println(new Date() + " -- Done");
	}
	
}
