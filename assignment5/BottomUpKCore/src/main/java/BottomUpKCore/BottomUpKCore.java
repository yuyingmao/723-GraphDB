package BottomUpKCore;

import java.io.File;
import java.util.Date;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class BottomUpKCore {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0],
				neo4jFolderCopy = args[1];

		System.out.println(new Date() + " -- Started");
		
		BatchInserter dbOrig = BatchInserters.inserter(new File(neo4jFolder));
		GraphDatabaseService dbCopy = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolderCopy))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M").newGraphDatabase();
		
		// TODO: Your code here!!!!
		// Both databases will contain the same graph at the beginning and you are supposed to keep removing nodes from the copy while
		//		updating the original database. Since we are just going to update the original database, BatchInserter will do the job
		//		much faster. You can use "MATCH (v) WITH v, size((v)--()) as degree" to compute the degree of the nodes in Cypher, you
		//		can use DETACH DELETE to remove nodes and their edges. You must fill the "psi" property in the original database.

		long total = 0;

		Transaction tx = dbCopy.beginTx();
		Result r = dbCopy.execute("MATCH (n) RETURN count(n)");
		while(r.hasNext()){
			total = ((Number) r.next().get("count(n)")).longValue();
		}
		tx.close();

		long current = 0;
		long totalBefore;

		while(total > 0){
			do {
				totalBefore = total;
				tx = dbCopy.beginTx();
				Result nodesToDelete = dbCopy.execute("MATCH (n) WITH ID(n) AS index, size((n) -- ()) AS ct WHERE ct < " + current + " RETURN index");
				while (nodesToDelete.hasNext()) {
					long node = ((Number) nodesToDelete.next().get("index")).longValue();
					total--;
					dbOrig.setNodeProperty(node, "psi", current - 1);
				}
				dbCopy.execute("MATCH (n) WITH n AS node, size((n) -- ()) AS ct WHERE ct < " + current + " DETACH DELETE node");
				tx.success();
				tx.close();
			} while (total != totalBefore);

			current++;
		}

		
		// TODO: End of your code.

		dbCopy.shutdown();
		dbOrig.shutdown();

		System.out.println(new Date() + " -- Done");
	}

}
