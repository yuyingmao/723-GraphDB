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

		//initially, set all relationships to have a property "split"="Training"
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

		for(String p:predicates){
			//a number used for splitting Grest into half
			long cnt=0;
			//get triples, subjects, and objects have this predicate
			long totalNumOfTriples=(long) db.execute("MATCH ()-[p:`$pred`]->() RETURN COUNT(p) AS c".replace("$pred", p)).next().get("c");
			long totalNumOfSubjects=(long)db.execute("MATCH (s)-[p:`$pred`]->() RETURN COUNT(DISTINCT s) AS c".replace("$pred", p)).next().get("c");
			long totalNumOfObjects=(long)db.execute("MATCH ()-[p:`$pred`]->(o) RETURN COUNT(DISTINCT o) AS c".replace("$pred", p)).next().get("c");
			//calculate original average indegree and outdegree
			double avgIndegree=(double)totalNumOfTriples/totalNumOfObjects;
			double avgOutdegree=(double)totalNumOfTriples/totalNumOfSubjects;

			try(Result rs=db.execute("MATCH (s)-[p:`$pred`]->(o) RETURN ID(s),ID(o),ID(p)".replace("$pred", p))){
				while(rs.hasNext()){
					Map<String,Object> row=rs.next();
					long sID=(long)row.get("ID(s)");
					long oID=(long)row.get("ID(o)");
					long pID=(long)row.get("ID(p)");

					tx=db.beginTx();
					if(db.getNodeById(sID).getDegree()>1&&db.getNodeById(oID).getDegree()>1){
						//update total number of subjects and objects
						long newTotalNumOfSubjects=totalNumOfSubjects;
						long newTotalNumOfObjects=totalNumOfObjects;
						try(Result result=db.execute("MATCH (s)-[p:`$pred`]->(o) WHERE ID(s)="+sID+" AND ID(o)<>"+oID+" RETURN ID(o)".replace("$pred", p))){
							if(!result.hasNext())
								newTotalNumOfSubjects-=1;
						}
						try(Result result=db.execute("MATCH (s)-[p:`$pred`]->(o) WHERE ID(s)<>"+sID+" AND ID(o)="+oID+" RETURN ID(s)".replace("$pred", p))){
							if(!result.hasNext())
								newTotalNumOfObjects-=1;
						}
						//calculate new average indegree and outdegree
						double newAvgIndegree=(double)(totalNumOfTriples-1)/newTotalNumOfObjects;
						double newAvgOutdegree=(double)(totalNumOfTriples-1)/newTotalNumOfSubjects;
						if(Math.abs(avgIndegree-newAvgIndegree)<toleranceThreshold
								&& Math.abs(avgOutdegree-newAvgOutdegree)<toleranceThreshold){
							//split Grest to validation or test half and half
							if(cnt%2==0)
								db.getRelationshipById(pID).setProperty("split","Validation");
							else
								db.getRelationshipById(pID).setProperty("split","Test");
							tx.success();
							cnt++;
						}
					}
					tx.close();
				}
			}
		}
		
		db.shutdown();

		System.out.println(new Date() + " -- Done");
	}

}
