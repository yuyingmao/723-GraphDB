package LCWA;
import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import org.neo4j.graphdb.Transaction;

public class LCWA {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final String relation2id = args[1];

		System.out.println(new Date() + " -- Started");
		
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jFolder));
		
		// Read predicates ids.
		Map<String, Long> predicates = new HashMap<>();
		Scanner sc = new Scanner(new File(relation2id));
		while (sc.hasNextLine()) {
			String[] line = sc.nextLine().split("\t");
			if (line.length == 2)
				predicates.put(line[0], Long.valueOf(line[1]));
		}
		sc.close();
		
		// TODO Your code here!
		// Use the predicate ids to create the properties, e.g., subjects_1_train, which are the subject in the training split
		//	for predicate 1.

		Transaction t = db.beginTx();

		for(String pred : predicates.keySet()) {
			db.execute("MATCH (s) -[p:" + pred + "]-> (n) WHERE p.split = 'Training'" +
					" WITH collect(ID(s)) AS subjects, n SET n.subjects_" + predicates.get(pred) + "_train = subjects");

			db.execute("MATCH (s) -[p:" + pred + "]-> (n) WHERE p.split = 'Training' OR p.split = 'Validation'" +
					" WITH collect(ID(s)) AS subjects, n SET n.subjects_" + predicates.get(pred) + "_valid = subjects");

			db.execute("MATCH (s) -[p:" + pred + "]-> (n) WHERE p.split = 'Training' OR p.split = 'Validation' OR p.split = 'Test'" +
					" WITH collect(ID(s)) AS subjects, n SET n.subjects_" + predicates.get(pred) + "_test = subjects");

			db.execute("MATCH (n) -[p:" + pred + "]-> (o) WHERE p.split = 'Training'" +
					" WITH collect(ID(o)) AS objects, n SET n.objects_" + predicates.get(pred) + "_train = objects");

			db.execute("MATCH (n) -[p:" + pred + "]-> (o) WHERE p.split = 'Training' OR p.split = 'Validation'" +
					" WITH collect(ID(o)) AS objects, n SET n.objects_" + predicates.get(pred) + "_valid = objects");

			db.execute("MATCH (n) -[p:" + pred + "]-> (o) WHERE p.split = 'Training' OR p.split = 'Validation' OR p.split = 'Test'" +
					" WITH collect(ID(o)) AS objects, n SET n.objects_" + predicates.get(pred) + "_test = objects");
		}

		t.success();
		t.close();

		// End testing

		
		// TODO End of your code.
		
		db.shutdown();

		System.out.println(new Date() + " -- Done");
	}

	private static boolean isCorrect(Map<String, Object> innerRow, String str, long other) {
		boolean ret = true;
		if (innerRow.get(str) == null)
			ret = other == 0;
		else
			ret = (long) innerRow.get(str) == other;
		return ret;
	}

}
