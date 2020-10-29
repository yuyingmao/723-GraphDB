package BottomUpKCore;

import java.io.File;
import java.util.Date;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class BottomUpKCore {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0],
				neo4jFolderCopy = args[1];

		System.out.println(new Date() + " -- Started");

		BatchInserter dbOrig = BatchInserters.inserter(new File(neo4jFolder));
		GraphDatabaseService dbCopy = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolderCopy))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M").newGraphDatabase();

		int total=((Number)dbCopy.execute("MATCH (n) RETURN COUNT(n)").next().get("COUNT(n)")).intValue();
		int current=0;
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
			try(Result rs=dbCopy.execute("MATCH (n) RETURN ID(n)")){
				while (rs.hasNext())
					dbOrig.setNodeProperty((long)rs.next().get("ID(n)"),"psi",current);
			}
			txCopy.close();
			current++;
		}

		dbCopy.shutdown();
		dbOrig.shutdown();

		System.out.println(new Date() + " -- Done");
	}

}
