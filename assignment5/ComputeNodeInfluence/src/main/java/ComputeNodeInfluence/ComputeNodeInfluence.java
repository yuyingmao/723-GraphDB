package ComputeNodeInfluence;

import java.io.File;
import java.util.*;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class ComputeNodeInfluence {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0];
		final long initial = Long.parseLong(args[1]);
		final int repeat = Integer.parseInt(args[2]);
		final double beta = Double.parseDouble(args[3]);

		System.out.println(new Date() + " -- Started");
		
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jFolder));

		double f=0;

		for (int i=0; i<repeat; i++) {
			System.out.println(new Date() + " -- Iteration: " + i);
			
			// TODO: Your code here!
			// Infect the initial node. The rest of the nodes must start in state S. In the initial node, property f will store its influence and
			//		property it will store the number of iterations your program has accomplished. At the end of each iteration, you must update
			//		both f and it with the current values.
			// In each step, each infected node will infect its immediate neighbors that are in state S with probability beta. Continue until
			//		there are no more infected nodes.
			Map<Long,String> nodeState=new HashMap<>();
			//set initial node's state to be "I"
			nodeState.put(initial,"I");
			//get all nodes that are not initial node and set their state to be S
			try(Result rs=db.execute("MATCH (n) WHERE ID(n)<>"+initial+" RETURN ID(n)")){
				while(rs.hasNext())
					nodeState.put((long)rs.next().get("ID(n)"),"S");
			}
			int fPremium=0;
			int step=0;
			while(nodeState.containsValue("I")){
				int cnt=0;
				for(long node:nodeState.keySet()){
					if(nodeState.get(node).equals("I")){
						//update that node's state to be "R"
						nodeState.put(node,"R");
						//get N(v)
						Set<Long> neighbors=new HashSet<>();
						try(Result rs=db.execute("MATCH (n)--(m) WHERE ID(n)="+node+" RETURN ID(m)")){
							while(rs.hasNext())
								neighbors.add((long)rs.next().get("ID(m)"));
						}
						//effects neighbors
						for(long m:neighbors){
							if(nodeState.get(m).equals("S") && new Random().nextDouble()<=beta){
								nodeState.put(m,"I");
								cnt++;
							}
						}
					}
				}
				fPremium+=cnt;
				step++;
			}
			f=f+(double)(fPremium/step);
			// TODO: End of your code.
		}
		//update f and it
		Transaction tx=db.beginTx();
		Node v=db.getNodeById(initial);
		v.setProperty("f",f/repeat);
		tx.success();
		v.setProperty("it",repeat);
		tx.success();
		tx.close();

		db.shutdown();

		System.out.println(new Date() + " -- Done");
	}
	
}
