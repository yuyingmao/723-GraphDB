package ModelEval;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.Result;

public class ModelEval {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final String file = args[1];
		final String norm = args[2];

		System.out.println(new Date() + " -- Started");
		
		// Positive first, negative rest.
		int current = 0;
		List<long[]> positiveAndNegatives = new ArrayList<>();
		Scanner sc = new Scanner(new File(file));
		while (sc.hasNextLine()) {
			String[] line = sc.nextLine().split("\t");
			
			long[] tuple = null;
			if (current == 0)
				tuple = new long[] {Long.valueOf(line[0]), Long.valueOf(line[1]), Long.valueOf(line[2]), Long.valueOf(line[3])};
			else
				tuple = new long[] {Long.valueOf(line[0]), Long.valueOf(line[1]), Long.valueOf(line[2])};
			
			positiveAndNegatives.add(tuple);
			current++;
		}
		sc.close();
		
		int lt = 1, eq = 0;
		double posScore = Double.NaN;
		long relid = -1;
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jFolder));
		Transaction tx = db.beginTx();

		for (int i = 0; i < positiveAndNegatives.size(); i++) {
			long[] triple = positiveAndNegatives.get(i);
			long s = triple[0], p = triple[1], o = triple[2];
			
			// TODO Your code here!
			// Note that triple[3] in the first triple (positive) contains the id of the relationship that has the embedding
			//	of predicate p.
			// posScore will be the score of the positive triple.
			// You must update lt and eq according to whether the score of each negative triple is strictly less (lt) or equal (eq)
			//	than posScore.
			// The distance is according to the norm provided as input.

			if(i == 0){
				relid = triple[3];
				double[] embS = (double[]) db.execute("MATCH (n) WHERE ID(n) = " + s + " RETURN n.embedding").next().get("n.embedding");
				double[] embP = (double[]) db.execute("MATCH () -[p]-> () WHERE ID(p) = " + p + " RETURN p.embedding").next().get("p.embedding");
				double[] embO = (double[]) db.execute("MATCH (n) WHERE ID(n) = " + o + " RETURN n.embedding").next().get("n.embedding");

				posScore = distance(embS, embP, embO, norm);
			}
			else{
				double[] embS = (double[]) db.execute("MATCH (n) WHERE ID(n) = " + s + " RETURN n.embedding").next().get("n.embedding");
				double[] embP = (double[]) db.execute("MATCH () -[p]-> () WHERE ID(p) = " + p + " RETURN p.embedding").next().get("p.embedding");
				double[] embO = (double[]) db.execute("MATCH (n) WHERE ID(n) = " + o + " RETURN n.embedding").next().get("n.embedding");

				double dist = distance(embS, embP, embO, norm);

				if(dist < posScore){
					lt++;
				}
				if(dist == posScore){
					eq++;
				}
			}

			// TODO End of your code.
		}
		db.getRelationshipById(relid).setProperty("rank", lt*1.0 + (eq*1.0/2.0));
		tx.success();
		tx.close();
		db.shutdown();

		System.out.println(new Date() + " -- Done");
	}

	private static double distance(double[] embS, double[] embP, double[] embO, String norm){
		boolean manhattan = norm.equals("l1");

		double sum = 0.0;

		for(int i = 0; i < embS.length; i++){
			if(manhattan){
				sum += Math.abs(embS[i] + embP[i] - embO[i]);
			}
			else{
				sum += Math.pow(embS[i] + embP[i] - embO[i], 2);
			}
		}

		if(manhattan){
			return sum;
		}
		else{
			return Math.sqrt(sum);
		}
	}

}
