package ComputeNodeInfluence;

import java.io.File;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class ComputeNodeInfluence {

	public static void main(String[] args){
		final String neo4jFolder = args[0];
		final long initial = Long.parseLong(args[1]);
		final int repeat = Integer.parseInt(args[2]);
		final double beta = Double.parseDouble(args[3]);

		System.out.println(new Date() + " -- Started");

		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jFolder));
		//initialize the initial node to have f of 0.0, it of 0, and state of I
		Transaction tx=db.beginTx();
		Node initV=db.getNodeById(initial);
		initV.setProperty("f",0.0);
		tx.success();
		initV.setProperty("it",0);
		tx.success();
		initV.setProperty("state","I");
		tx.success();
		tx.close();


		for (int i=1; i<repeat+1; i++) {
			System.out.println(new Date() + " -- Iteration: " + i);
			//initial or reset all nodes besides the initial node to have state of S
			tx=db.beginTx();
			db.execute("MATCH (n) WHERE ID(n)<>"+initial+" SET n.state='S'");
			tx.success();
			tx.close();
			int fThisIter=0;
			int step=0;
			while(true){
				Set<Long> iNodes=new HashSet<>();
				//get all nodes with a state of I
				try(Result rs=db.execute("MATCH (n) WHERE n.state='I' RETURN ID(n)")){
					while (rs.hasNext())
						iNodes.add((long)rs.next().get("ID(n)"));
				}
				if(iNodes.isEmpty())
					break;
				int cnt=0;
				for(long node:iNodes){
					//update its state to be R
					tx=db.beginTx();
					db.getNodeById(node).setProperty("state","R");
					tx.success();
					tx.close();
					//get N(v) whose state is S
					Set<Long> neighbors=new HashSet<>();
					try(Result rs=db.execute("MATCH (n)--(m) WHERE ID(n)="+node+" AND m.state='S' RETURN ID(m)")){
						while(rs.hasNext())
							neighbors.add((long)rs.next().get("ID(m)"));
					}
					//affects neighbors based on the infection rate
					for(long m:neighbors){
						if(new Random().nextDouble()<=beta){
							cnt++;
							tx=db.beginTx();
							db.getNodeById(m).setProperty("state","I");
							tx.success();
							tx.close();
						}
					}
				}
				fThisIter+=cnt;
				step++;
			}
			//update f and it
			tx=db.beginTx();
			initV=db.getNodeById(initial);
			double f=(double)initV.getProperty("f");
			f=f+(double)(fThisIter/step);
			initV.setProperty("f", f/i);
			tx.success();
			initV.setProperty("it",i);
			tx.success();
			//reset initial node's state to be I
			initV.setProperty("state","I");
			tx.success();
			tx.close();

		}

		db.shutdown();

		System.out.println(new Date() + " -- Done");
	}

}
