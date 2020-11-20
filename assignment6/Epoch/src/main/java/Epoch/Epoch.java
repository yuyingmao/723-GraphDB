package Epoch;

import java.io.File;
import java.util.*;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class Epoch {

	private static double Manhattan(int dim,double[] sEmbedding,double[] pEmbedding,double[] oEmbedding){
		double distance=0;
		for(int i=0;i<dim;i++)
			distance+=Math.abs(sEmbedding[i]+pEmbedding[i]-oEmbedding[i]);
		return distance;
	}

	private static double Euclidean(int dim,double[] sEmbedding,double[] pEmbedding,double[] oEmbedding){
		double distance=0;
		for(int i=0;i<dim;i++)
			distance+=Math.pow(sEmbedding[i]+pEmbedding[i]-oEmbedding[i],2);
		return Math.sqrt(distance);
	}

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0]; //Neo4j database, Gtr, Gva
		final String batchFile = args[1];
		final double alpha = Double.parseDouble(args[2]);
		final double gamma = Double.parseDouble(args[3]);
		final String norm = args[4];

		System.out.println(new Date() + " -- Started");

		// Batch.
		List<long[]> batch = new ArrayList<>();
		Scanner sc = new Scanner(new File(batchFile));
		while (sc.hasNextLine()) {
			String[] line = sc.nextLine().split("\t");
			batch.add(new long[] {Long.parseLong(line[0]), Long.parseLong(line[1]), Long.parseLong(line[2]), Long.parseLong(line[3]), Long.parseLong(line[4])});
		}
		sc.close();
		
		BatchInserter inserter = BatchInserters.inserter(new File(neo4jFolder));
		for (long[] tuple : batch) {
			long s = tuple[0], p = tuple[1], o = tuple[2], sp = tuple[3], op = tuple[4];

			// To speed up processing, use parallel and parallelStream when performing array computations.


			double[] sEmbedding= (double[])inserter.getNodeProperties(s).get("embedding");
			double[] oEmbedding= (double[])inserter.getNodeProperties(o).get("embedding");
			double[] spEmbedding=null, opEmbedding=null;
			if(sp==s)
				spEmbedding=sEmbedding;
			else
				spEmbedding=(double[])inserter.getNodeProperties(sp).get("embedding");
			if(op==o)
				opEmbedding=oEmbedding;
			else
				opEmbedding=(double[])inserter.getNodeProperties(op).get("embedding");
			double[] pEmbedding= (double[])inserter.getRelationshipProperties(p).get("embedding");

			//calculate d(s+p,o) and d(s'+p,o')
			int dim=sEmbedding.length;
			double spoDistance=0, spoDistanceP=0;
			if(norm.equals("l1")){
				spoDistance=Manhattan(dim,sEmbedding,pEmbedding,oEmbedding);
				spoDistanceP=Manhattan(dim,spEmbedding,pEmbedding,opEmbedding);
			}
			else if(norm.equals("l2")){
				spoDistance=Euclidean(dim,sEmbedding,pEmbedding,oEmbedding);
				spoDistanceP=Euclidean(dim,spEmbedding,pEmbedding,opEmbedding);
			}
			//currently is using: gamma-d(s+p,o)+d(sp+p,op)
			if(gamma+spoDistance-spoDistanceP>0){
				for(int i=0;i<dim;i++){
					double si=sEmbedding[i];
					double pi=pEmbedding[i];
					double oi=oEmbedding[i];
					double spi=spEmbedding[i];
					double opi=opEmbedding[i];
					double x=2*(si+pi-oi);
					double xp=2*(spi+pi-opi);
					if(norm.equals("l1")){
						sEmbedding[i]=si-alpha*Math.signum(x);
						oEmbedding[i]=oi+alpha*Math.signum(x);
						spEmbedding[i]=spi+alpha*Math.signum(xp);
						opEmbedding[i]=opi-alpha*Math.signum(xp);
						pEmbedding[i]=pi-alpha*Math.signum(x)+alpha*Math.signum(xp);
					}
					else if(norm.equals("l2")){
						sEmbedding[i]=si-alpha*x;
						oEmbedding[i]=oi+alpha*x;
						spEmbedding[i]=spi+alpha*xp;
						opEmbedding[i]=opi-alpha*xp;
						pEmbedding[i]=pi-alpha*x+alpha*xp;
					}
				}
				inserter.setNodeProperty(s,"embedding",sEmbedding);
				inserter.setNodeProperty(o,"embedding",oEmbedding);
				inserter.setNodeProperty(sp,"embedding",spEmbedding);
				inserter.setNodeProperty(op,"embedding",opEmbedding);
				inserter.setRelationshipProperty(p,"embedding",pEmbedding);
			}
		}
		inserter.shutdown();

		System.out.println(new Date() + " -- Done");

	}

}
