package KGSplit;

import java.io.File;
import java.util.*;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class KGSplit {
	public static void main(String[] args) {
		final String neo4jFolder = args[0];
		final double toleranceThreshold = Double.parseDouble(args[1]);

		System.out.println(new Date() + " -- Started");
		
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jFolder));

		//Gtr=G; Grest={}
		Transaction tx=db.beginTx();
		db.execute("MATCH ()-[p]->() SET p.split='Training'");
		tx.success();
		tx.close();

		//get all predicates
		Set<String> predicates=new HashSet<>();
		tx=db.beginTx();
		for (RelationshipType relationshipType : db.getAllRelationshipTypes()) {
			predicates.add(relationshipType.toString());
		}
		tx.close();

		//foreach p in P(G) do
		for(String p:predicates){
			// get count of subjects, objects, and triples in training
			long subjectsInTraining = 0, objectsInTraining = 0, triplesInTraining = 0;
			try(Result rs=db.execute("MATCH (s)-[p:`$pred` {split:'Training'}]->(o) RETURN COUNT(DISTINCT s) AS sCount, COUNT(DISTINCT o) AS oCount, COUNT(p) AS pCount".replace("$pred", p))){
				if(rs.hasNext()) {
					Map<String, Object> row = rs.next();
					subjectsInTraining=(long)row.get("sCount");
					objectsInTraining=(long)row.get("oCount");
					triplesInTraining=(long)row.get("pCount");
				}
			}
			//calculate original average indegree and outdegree
			double avgIndegree=(double)triplesInTraining/objectsInTraining;
			double avgOutdegree=(double)triplesInTraining/subjectsInTraining;

			tx=db.beginTx();
			try(Result rs=db.execute("MATCH (s)-[p:`$pred`]->(o) RETURN ID(s),ID(o),ID(p)".replace("$pred", p))){
				//a number used for splitting Grest into half
				long cnt=0;
				//foreach (s,p,o) in G do
				while(rs.hasNext()){
					Map<String,Object> row=rs.next();
					long sID=(long)row.get("ID(s)");
					long oID=(long)row.get("ID(o)");
					int sDegreeInTraining=((Number)db.execute("MATCH (e)-[p {split:'Training'}]-() WHERE ID(e)=$e RETURN COUNT(p) AS degree".replace("$e",String.valueOf(sID))).next().get("degree")).intValue();
					int oDegreeInTraining=((Number)db.execute("MATCH (e)-[p {split:'Training'}]-() WHERE ID(e)=$e RETURN COUNT(p) AS degree".replace("$e",String.valueOf(oID))).next().get("degree")).intValue();
					//if E(G'tr)=E(G) then
					if(sDegreeInTraining>1 && oDegreeInTraining>1){
						//update count of subjects and objects
						long newSubjectsInTraining=subjectsInTraining;
						long newObjectsInTraining=objectsInTraining;
						int sDegreeInP=((Number)db.execute("MATCH (s)-[p:`$pred` {split:'Training'}]->() WHERE ID(s)=$s RETURN COUNT(p) AS degree".replace("$s",String.valueOf(sID)).replace("$pred", p)).next().get("degree")).intValue();
						if(sDegreeInP==1)
							newSubjectsInTraining-=1;
						int oDegreeInP=((Number)db.execute("MATCH ()-[p:`$pred` {split:'Training'}]->(o) WHERE ID(o)=$o RETURN COUNT(p) AS degree".replace("$o",String.valueOf(oID)).replace("$pred", p)).next().get("degree")).intValue();
						if(oDegreeInP==1)
							newObjectsInTraining-=1;
						//calculate new average indegree and outdegree
						double newAvgIndegree=(double)(triplesInTraining-1)/newObjectsInTraining;
						double newAvgOutdegree=(double)(triplesInTraining-1)/newSubjectsInTraining;
						if(Math.abs(avgIndegree-newAvgIndegree)<toleranceThreshold && Math.abs(avgOutdegree-newAvgOutdegree)<toleranceThreshold){
							long pID=(long)row.get("ID(p)");

							//split Grest to validation or test half and half

							if(cnt%2==0)
								db.getRelationshipById(pID).setProperty("split","Validation");
							else
								db.getRelationshipById(pID).setProperty("split","Test");
							tx.success();

							cnt++;

							triplesInTraining-=1;
							subjectsInTraining=newSubjectsInTraining;
							objectsInTraining=newObjectsInTraining;
						}
					}
				}
			}catch (Exception e){
				tx.close();
				e.printStackTrace();
			}
			tx.close();
		}
		
		db.shutdown();

		System.out.println(new Date() + " -- Done");
	}

}
