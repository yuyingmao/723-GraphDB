package KGSplit;

import java.io.File;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javafx.util.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
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
		try(Result rs=db.execute("MATCH ()-[p]->() RETURN DISTINCT TYPE(p)")){
			while(rs.hasNext())
				predicates.add((String)rs.next().get("TYPE(p)"));
		}

		//a number used for splitting Grest into half
		long cnt=0;

		for(String p:predicates){
			//get triples, subjects, and objects have this predicate
			Set<Pair<Long,Long>> triples=new HashSet<>();
			try(Result rs=db.execute("MATCH (s)-[p:`$pred`]->(o) RETURN ID(s),ID(o)".replace("$pred", p))){
				while(rs.hasNext()){
					Map<String,Object> row=rs.next();
					triples.add(new Pair<>((long)row.get("ID(s)"),(long)row.get("ID(o)")));
				}
			}
			long totalNumOfTriples=triples.size();
			long totalNumOfSubjects=(long)db.execute("MATCH (s)-[p:`$pred`]->() RETURN COUNT(DISTINCT s) AS c".replace("$pred", p)).next().get("c");
			long totalNumOfObjects=(long)db.execute("MATCH ()-[p:`$pred`]->(o) RETURN COUNT(DISTINCT o) AS c".replace("$pred", p)).next().get("c");
			//calculate original average indegree and outdegree
			double avgIndegree=(double)totalNumOfTriples/totalNumOfObjects;
			double avgOutdegree=(double)totalNumOfTriples/totalNumOfSubjects;
			for(Pair<Long,Long> triple:triples){
				long sID=triple.getKey();
				long oID=triple.getValue();
				//TODO: Check that s and o will not have a degree of zero in training => If so, go to next triple.
				tx=db.beginTx();
				if(db.getNodeById(sID).getDegree()>1&&db.getNodeById(oID).getDegree()>1){
					//update total number of subjects and objects
					long newTotalNumOfSubjects=totalNumOfSubjects;
					long newTotalNumOfObjects=totalNumOfObjects;
					try(Result rs=db.execute("MATCH (s)-->(o) WHERE ID(s)="+sID+" AND ID(o)<>"+oID+" RETURN ID(o)")){
						if(!rs.hasNext())
							newTotalNumOfSubjects-=1;
					}
					try(Result rs=db.execute("MATCH (s)-->(o) WHERE ID(s)<>"+sID+" AND ID(o)="+oID+" RETURN ID(s)")){
						if(!rs.hasNext())
							newTotalNumOfObjects-=1;
					}
					//calculate new average indegree and outdegree
					double newAvgIndegree=(double)(totalNumOfTriples-1)/newTotalNumOfObjects;
					double newAvgOutdegree=(double)(totalNumOfTriples-1)/newTotalNumOfSubjects;
					if(Math.abs(avgIndegree-newAvgIndegree)<toleranceThreshold
							&& Math.abs(avgOutdegree-newAvgOutdegree)<toleranceThreshold){
						long pID=(long)db.execute("MATCH (s)-[p]->(o) WHERE ID(s)="+sID
								+" AND ID(o)="+oID+" RETURN ID(p)").next().get("ID(p)");
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
		
		db.shutdown();

		System.out.println(new Date() + " -- Done");
	}

}
