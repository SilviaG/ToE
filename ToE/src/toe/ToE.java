package toe;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class ToE {
	
	private static HashMap<Resource,List<ArrayList<RDFNode>>> CS = new HashMap<Resource,List<ArrayList<RDFNode>>>();
	private static HashMap<String,Integer> S = new HashMap<String,Integer>();
	private static HashMap<String,Resource> StoCS = new HashMap<String,Resource>();
	private static List<String> endpoint_service = new ArrayList<String>();

	private static long sTime;
	private static long eTime;

	private static long sCQTime;
	private static long eCQTime;
	private static long sEQTime;
	private static long eEQTime;

	private static long sigma_a = 0;
	private static long sigma_b = 0;	

	private static int triple_a = 0;
	private static int triple_b = 0;

	private static String select = "select distinct ?s0 WHERE {";
	private static String where = "?s0 a <http://dati.camera.it/ocd/deputato> . ?s0 <http://dati.camera.it/ocd/rif_leg> <http://dati.camera.it/ocd/legislatura.rdf/repubblica_10> . ";
	private static String cluster = select+where+"}";

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		clustering();
	}
	
	public static void clustering() {
		
		endpoint_service.add("http://dati.camera.it/sparql");
		 


				ArrayList<Resource> R = doQuery(cluster);
				System.out.println(R.size());

				
				String[] URIab1 = {"http://dati.camera.it/ocd/deputato.rdf/d3140_10", "http://dati.camera.it/ocd/deputato.rdf/d270_10"};
				String[] URIab2 = {"http://dati.camera.it/ocd/deputato.rdf/d200023_10", "http://dati.camera.it/ocd/deputato.rdf/d22710_10"};
				String[] URIab3 = {"http://dati.camera.it/ocd/deputato.rdf/d30010_10", "http://dati.camera.it/ocd/deputato.rdf/d17060_10"};
				/*String[] URIab4 = {"http://dati.camera.it/ocd/deputato.rdf/d3650_10", "http://dati.camera.it/ocd/deputato.rdf/d31040_10"};
				String[] URIab5 = {"http://dati.camera.it/ocd/deputato.rdf/d15390_15", "http://dati.camera.it/ocd/deputato.rdf/d301523_15"};
				String[] URIab6 = {"http://dati.camera.it/ocd/deputato.rdf/d12140_1", "http://dati.camera.it/ocd/deputato.rdf/d8520_1"};*/

				ArrayList<String[]> pairs = new ArrayList<String[]>();
				pairs.add(URIab1);
				pairs.add(URIab2);
				pairs.add(URIab3);
				/*pairs.add(URIab4);
				pairs.add(URIab5);
				pairs.add(URIab6);*/
				//Iterator ip = pairs.iterator();
				Resource resource_a = ResourceFactory.createResource();
				Resource resource_b = ResourceFactory.createResource();
				String a = new String();
				String b = new String();
				int pp = 0;
				
				int n_a = 1;
				int n_b = 1;
				
				int f = 0;

				while(R.size()>1){
					
					f++;

					if(pp<pairs.size()){
						
						String[] pair = pairs.get(pp);
						pp++;

						String URIa = pair[0];
						String URIb = pair[1];

						int lIa = URIa.lastIndexOf("/");
						int lIb = URIb.lastIndexOf("/");

						a = URIa.substring(lIa+1);
						b = URIb.substring(lIb+1);

						//QUI ANDREBBE LA CHOOSE DA R
						resource_a = ResourceFactory.createResource(URIa);
						resource_b = ResourceFactory.createResource(URIb);

						
					}
					else{
						
						if(R.size()>2){
							double s1 = Math.random() * (R.size()-1);
							double s2 = Math.random() * (R.size()-1);
							resource_a = R.get((int)s1);
							while((int)s1 == (int)s2)
								s2 = Math.random() * (R.size()-1);
							resource_b = R.get((int)s2);
						}
						else{
							if(R.size()==2){
								resource_a = R.get(0);
								resource_b = R.get(1);
							}
							else{
								resource_a = R.get(0);
								resource_b = R.get(0);
							}
							
						}

						int lIa = resource_a.toString().lastIndexOf("/");
						int lIb = resource_b.toString().lastIndexOf("/");
						a = resource_a.toString().substring(lIa+1);
						b = resource_b.toString().substring(lIb+1);

					}
				

					sTime = System.currentTimeMillis();
					System.out.println("START TIME: "+sTime);

					
					ArrayList<Resource> r = CS(a, b, resource_a,resource_b,n_a,n_b);
					
					r.retainAll(R);
					System.out.println("r size: "+r.size());
					

				
					
					if(!r.contains(resource_a) || !r.contains(resource_b)){

						System.out.println("CLUSTERING ERROR!");

					}
					else{
						R.removeAll(r);
						System.out.println("R size: "+R.size());
					}

					try{
						PrintStream outputn = new PrintStream(new FileOutputStream("target/lod/clusters/v1-CS_"+n_a+"_"+f+"_"+a+"_"+b+"_D"+endpoint_service.size()+".txt"));


						Iterator it = CS.keySet().iterator();
						while(it.hasNext()){
							Resource x = (Resource) it.next();
							outputn.println("\nx: "+x);
							List<ArrayList<RDFNode>> list = new ArrayList<ArrayList<RDFNode>>();
							list = CS.get(x);
							Iterator l = list.iterator();
							while(l.hasNext()){
								ArrayList<RDFNode> yz = (ArrayList<RDFNode>) l.next();
								outputn.println("y: "+yz.get(0)+" z: "+yz.get(1));

							}
						}

						outputn.println("\nTOTAL CS TIME: "+(eTime-sTime));	

						outputn.println("\nTOTAL TIME compute_a: "+sigma_a);
						outputn.println("TOTAL TIME compute_b: "+sigma_b);

						outputn.println("\nQUERY TOTALI EFFETTUATE PER a: "+triple_a);
						outputn.println("QUERY TOTALI EFFETTUATE PER b: "+triple_b);

						outputn.println("\nTOTAL CLUSTERING TIME: "+(eEQTime-sCQTime));
						outputn.println("\nCOMPILE QUERY TIME: "+(eCQTime-sCQTime));
						outputn.println("\nEXECUTE QUERY TIME: "+(eEQTime-sEQTime));

						outputn.println("\nCLUSTER CARDINALITY: "+r.size());
						outputn.println("\nRESOURCES: "+r);

						outputn.close();
						
						r.clear();
						
						CS.clear();
						sigma_a = 0;
						sigma_b = 0;	
						triple_a = 0;
						triple_b = 0;
						S.clear();
						StoCS.clear();

					}catch(Exception e){

						System.out.println("errore scrittura su file: "+e);

					}

					

				}
				
				if(R.size()==1){
					
					f++;
					
					resource_a = R.get(0);
					resource_b = R.get(0);
			
					int lIa = resource_a.toString().lastIndexOf("/");
					int lIb = resource_b.toString().lastIndexOf("/");
					a = resource_a.toString().substring(lIa+1);
					b = resource_b.toString().substring(lIb+1);
					
					sTime = System.currentTimeMillis();
					System.out.println("START TIME: "+sTime);

					
					ArrayList<Resource> r = CS(a, b, resource_a,resource_b,n_a,n_b);
					r.retainAll(R);
					System.out.println(r.size());
					System.out.println(r);

					//eTime = System.currentTimeMillis();
					System.out.println("END TIME: "+eTime);
					System.out.println("TOTAL TIME: "+(eTime-sTime));
					
					if(!r.contains(resource_a) || !r.contains(resource_b)){

						System.out.println("CLUSTERING ERROR!");

					}
					
					try{
						PrintStream outputn = new PrintStream(new FileOutputStream("target//clusters/v1-CS_"+n_a+"_"+f+"_"+a+"_"+b+"_D"+endpoint_service.size()+".txt"));

						
						Iterator it = CS.keySet().iterator();
						while(it.hasNext()){
							Resource x = (Resource) it.next();
							outputn.println("\nx: "+x);
							List<ArrayList<RDFNode>> list = new ArrayList<ArrayList<RDFNode>>();
							list = CS.get(x);
							Iterator l = list.iterator();
							while(l.hasNext()){
								ArrayList<RDFNode> yz = (ArrayList<RDFNode>) l.next();
								outputn.println("y: "+yz.get(0)+" z: "+yz.get(1));

							}
						}

						outputn.println("\nTOTAL TIME: "+(eTime-sTime));	

						outputn.println("\nTOTAL TIME compute_a: "+sigma_a);
						outputn.println("TOTAL TIME compute_b: "+sigma_b);

						outputn.println("\nQUERY TOTALI EFFETTUATE PER a: "+triple_a);
						outputn.println("QUERY TOTALI EFFETTUATE PER b: "+triple_b);

						outputn.println("\nTOTAL CLUSTERING TIME: "+(eEQTime-sCQTime));
						outputn.println("\nCOMPILE QUERY TIME: "+(eCQTime-sCQTime));
						outputn.println("\nEXECUTE QUERY TIME: "+(eEQTime-sEQTime));

						outputn.println("\nCLUSTER CARDINALITY: "+r.size());
						outputn.println("\nRESOURCES: "+r);

						outputn.close();
						
						r.clear();
						
						CS.clear();
						sigma_a = 0;
						sigma_b = 0;	
						triple_a = 0;
						triple_b = 0;
						S.clear();
						StoCS.clear();

					}catch(Exception e){

						System.out.println("errore scrittura su file: "+e);

					}

					
					
					
				}
				

		
	}
	
	public static ArrayList<Resource> doQuery(String querystring) {
		// TODO Auto-generated method stub
		
		String rs = "?s0";
		Query query = QueryFactory.create(querystring);
		List<QuerySolution> answer = new ArrayList<QuerySolution>();
		

		Iterator<String> s = endpoint_service.iterator();
		while(s.hasNext()){
			QueryExecution x = QueryExecutionFactory.sparqlService((String) s.next(), query);
			ResultSet results = x.execSelect();
			answer.addAll(ResultSetFormatter.toList(results));
		}

		Iterator<QuerySolution> ia = answer.iterator();
		ArrayList<Resource> r = new ArrayList<Resource>();
		while(ia.hasNext()){

			QuerySolution qs = (QuerySolution) ia.next();
			
			

			if(qs.get(rs).isResource()){

				r.add(qs.getResource(rs));
			}
		}
		
		
		
		return r;

		
	}

	public static ArrayList<Resource> CS(String sa, String sb, Resource a, Resource b, int n_a, int n_b){ //String endpoint_service){

		
		
		long sigma_aS = System.currentTimeMillis();
		HashMap<Resource,List<ArrayList<RDFNode>>> aT_a = compute_sigma(a,n_a);//,endpoint_service);
		

		
		triple_a++;
		long sigma_aE = System.currentTimeMillis();
		sigma_a = sigma_a + (sigma_aE - sigma_aS);
		System.out.println("a: "+aT_a.size());

		long sigma_bS = System.currentTimeMillis();
		HashMap<Resource,List<ArrayList<RDFNode>>> bT_b = compute_sigma(b,n_b);//,endpoint_service);
		
		
		
		triple_b++;
		long sigma_bE = System.currentTimeMillis();
		sigma_b = sigma_b + (sigma_bE - sigma_bS);
		System.out.println("b: "+bT_b.size());

		Resource x = ResourceFactory.createResource();
		
		
		String blank_a = "";
		String blank_b = "";
		HashMap<Resource,List<ArrayList<RDFNode>>> xT_x = explore(blank_a,blank_b,x, a, aT_a.get(a), b, bT_b.get(b), n_a, n_b, false);
		x = xT_x.keySet().iterator().next();
		String[] uriCS = {a.toString(), b.toString()};
		Arrays.sort(uriCS,String.CASE_INSENSITIVE_ORDER);
		String uristring = uriCS[0]+uriCS[1];
		S.put(uristring,1);
		CS.putAll(xT_x);
		StoCS.put(uristring,x);
		
		eTime = System.currentTimeMillis();
		
		System.out.println("size xTx: "+xT_x.keySet().size());
		System.out.println("END EXPLORE TIME: "+eTime);
		System.out.println("TOTAL EXPLORE TIME: "+(eTime-sTime));
		
		sCQTime = System.currentTimeMillis();
		int fn = compileQuery(x, n_a, sa, sb);
		
		eCQTime = System.currentTimeMillis();
		
		sEQTime = System.currentTimeMillis();
		ArrayList<Resource> r = executeQueries(fn,n_a, sa, sb);
		eEQTime = System.currentTimeMillis();
		
		
		

		return r; 

	}

	public static HashMap<Resource,List<ArrayList<RDFNode>>> compute_sigma(Resource resource, int n_r){//, String endpoint_service){

		HashMap<Resource,List<ArrayList<RDFNode>>> rgraph = new HashMap<Resource,List<ArrayList<RDFNode>>>();
		List<ArrayList<RDFNode>> T_r = new ArrayList<ArrayList<RDFNode>>();


		if(n_r>0){

			String p = "?p";
			String o = "?o";

			
			String querystring =
				
					"SELECT DISTINCT "+p+" "+o+" \n" +
					"WHERE\n" +
					"  { " +
					"<"+resource.getURI()+"> "+p+" "+o+" .\n" +
				
				
					"FILTER(?p != <http://dbpedia.org/ontology/wikiPageWikiLink>) \n" +
					"FILTER(?p != <http://purl.org/dc/elements/1.1/title>) \n" +
					"FILTER(?p != <http://dati.camera.it/ocd/endDate>) \n" +
					"FILTER(?p != <http://dati.camera.it/ocd/startDate>) \n" +
					"FILTER(?p != <http://purl.org/dc/elements/1.1/date>) \n" +
					"FILTER(?p != <http://dati.camera.it/ocd/file>) \n" +
					"FILTER(?p != <http://www.w3.org/2000/01/rdf-schema#label>) \n" +
					"FILTER(?p != <http://purl.org/dc/terms/isReferencedBy>) \n" +
					"FILTER(?p != <http://lod.xdams.org/ontologies/ods/modified>) \n" +
					"FILTER(?p != <http://xmlns.com/foaf/0.1/nick>) \n" +
					"FILTER(?p != <http://xmlns.com/foaf/0.1/depiction>) \n" +
					"FILTER(?p != <http://xmlns.com/foaf/0.1/firstName>) \n" +
					"FILTER(?p != <http://xmlns.com/foaf/0.1/surname>) \n" +
					"FILTER(?p != <http://www.w3.org/2000/01/rdf-schema#comment>) \n" +
					
					"  } \n" +
					
					"";
					
			Query query = QueryFactory.create(querystring);
			List<QuerySolution> answer = new ArrayList<QuerySolution>();

			Iterator<String> s = endpoint_service.iterator();
			while(s.hasNext()){
				QueryExecution x = QueryExecutionFactory.sparqlService((String) s.next(), query);
				ResultSet results = x.execSelect();
				answer.addAll(ResultSetFormatter.toList(results));
			}

			HashMap<Resource,Boolean> pb = new HashMap<Resource,Boolean>();
			Iterator<QuerySolution> ia = answer.iterator();
			while(ia.hasNext()){

				QuerySolution po = (QuerySolution) ia.next();
				ArrayList<RDFNode> spo = new ArrayList<RDFNode>();

				if(po.get(p).isResource() && !po.get(p).isAnon()){

					if(po.get(o).isResource() && !po.get(o).isAnon()){
						spo.add(po.getResource(p));
						spo.add(po.getResource(o));
						T_r.add(spo);
					}
					else{
						if(po.get(o).isAnon()){
							pb.put(po.get(p).asResource(), Boolean.TRUE);
						}
						else{
							spo.add(po.getResource(p));
							spo.add(po.getLiteral(o));	
							T_r.add(spo);
						}
						
					}

				}

			}
			
			Iterator<Resource> ipb = pb.keySet().iterator();
			while(ipb.hasNext()){

				ArrayList<RDFNode> spo = new ArrayList<RDFNode>();
				Resource pbo = ipb.next();
				spo.add(pbo);	
				spo.add(ResourceFactory.createResource());	
				T_r.add(spo);
				
			}

		}

		rgraph.put(resource, T_r);
		return rgraph;

		
	}

	public static HashMap<Resource,List<ArrayList<RDFNode>>> explore(String blank_a,String blank_b, Resource x, Resource r1, List<ArrayList<RDFNode>> T_r1, Resource r2, List<ArrayList<RDFNode>> T_r2, int n1, int n2, boolean cycle){//, HashMap<Integer,List<ArrayList<RDFNode>>> S){
		
		HashMap<Resource,List<ArrayList<RDFNode>>> xT_x = new HashMap<Resource,List<ArrayList<RDFNode>>>();
		List<ArrayList<RDFNode>> T_x = new ArrayList<ArrayList<RDFNode>>(); 

		if(!cycle){
			

				

			String[] uriCS = {r1.toString(), r2.toString()};
			Arrays.sort(uriCS, String.CASE_INSENSITIVE_ORDER);
			String uristring = uriCS[0]+uriCS[1];	
			if(r1.equals(r2)){
				x = r1;
				
			}
			xT_x.put(x,T_x);

			S.put(uristring,0);
			CS.putAll((Map<? extends Resource, ? extends List<ArrayList<RDFNode>>>) xT_x.clone());
			StoCS.put(uristring, x);

		}
		else{
			T_r1 = new ArrayList<ArrayList<RDFNode>>(); 
			T_r2 = new ArrayList<ArrayList<RDFNode>>();
		}



		if(r1.equals(r2)){

			x = r1;
			

						
			xT_x.put(x, T_r1);
			return xT_x;

		}


		if(T_r1.size()==0 || T_r2.size()==0){

			xT_x.put(x, T_x);

			return xT_x;
		}

		int w=0;
		Iterator<ArrayList<RDFNode>> it1 = T_r1.iterator();
		while (it1.hasNext()){
			
			String stat_r1 = new String();
			String stat_r2 = new String();
			
			if(!r1.isAnon())
				stat_r1 = " <" + r1 + "> ";
			else
				stat_r1 = " ?ob"+n1+" ";
			

			w++;

			List<RDFNode> pc = new ArrayList<RDFNode>();
			pc = (List<RDFNode>) it1.next().clone();
			RDFNode p = pc.get(0);
			RDFNode c = pc.get(1);

			triple_a++;

			HashMap<Resource,List<ArrayList<RDFNode>>> pT_p = new HashMap<Resource,List<ArrayList<RDFNode>>>();
			if(n1-1>0){

				
				stat_r1 = stat_r1 + " <" + p + ">";
				

				long sigma_aS = System.currentTimeMillis();
				pT_p.putAll(compute_sigma(p.asResource(),n1-1));
				long sigma_aE = System.currentTimeMillis();
				sigma_a = sigma_a + (sigma_aE - sigma_aS);

			}
			else{
				pT_p.put(p.asResource(), new ArrayList<ArrayList<RDFNode>>());
			}


			HashMap<Resource,List<ArrayList<RDFNode>>> cT_c = new   HashMap<Resource,List<ArrayList<RDFNode>>>();
			Set<Resource> sc = new HashSet<Resource>();
			if(!c.isLiteral()){
	
				triple_a++;

				if(n1-1>0){
					long sigma_aS = System.currentTimeMillis();
					if(!c.isAnon()){

						
						stat_r1 = stat_r1 + " <" + c + "> .";
						

						cT_c.putAll(compute_sigma(c.asResource(),n1-1));
						sc.addAll(cT_c.keySet());
					}
					else{
						
						stat_r1 = stat_r1 + " ?ob"+(n1-1)+" .";
						
						cT_c.putAll(compute_sigma_blank(blank_a+stat_r1,c.asResource(),n1-1));
						sc.addAll(cT_c.keySet());
					}
					long sigma_aE = System.currentTimeMillis();
					sigma_a = sigma_a + (sigma_aE - sigma_aS);
				}
				else{
					cT_c.put(c.asResource(), new ArrayList<ArrayList<RDFNode>>());
					sc.addAll(cT_c.keySet());
				}


			}


			Iterator<ArrayList<RDFNode>> it2 = T_r2.iterator();
			while (it2.hasNext()){
				
				stat_r2 = new String();

				if(!r2.isAnon())
					stat_r2 = " <" + r2 + "> ";
				else
					stat_r2 = " ?ob"+n2+" ";
				

				List<RDFNode> qd = (List<RDFNode>) it2.next();

				RDFNode q = qd.get(0);
				RDFNode d = qd.get(1);

				triple_b++;

				HashMap<Resource,List<ArrayList<RDFNode>>> qT_q = new HashMap<Resource,List<ArrayList<RDFNode>>>();
				if(n2-1>0){

					
					stat_r2 = stat_r2 + " <" + q + ">";

					long sigma_bS = System.currentTimeMillis();
					qT_q.putAll(compute_sigma(q.asResource(),n2-1)); 
					long sigma_bE = System.currentTimeMillis();
					if(w==1)
						sigma_b = sigma_b + (sigma_bE - sigma_bS);
				}
				else{
					qT_q.put(q.asResource(), new ArrayList<ArrayList<RDFNode>>());
				}		


				HashMap<Resource,List<ArrayList<RDFNode>>> pqT_pq = new HashMap<Resource,List<ArrayList<RDFNode>>>();
				String[] stringp = {p.toString(), q.toString()};
				Arrays.sort(stringp, String.CASE_INSENSITIVE_ORDER);
				String ps = stringp[0]+stringp[1];
				

				Resource y = ResourceFactory.createResource();

				boolean contained = false;
			

				if(S.containsKey(ps)){

					contained = true;
					y = StoCS.get(ps);

				}

				if(!contained){	
					
					
					pqT_pq = explore("","",y, p.asResource(), pT_p.get(p), q.asResource(), qT_q.get(q), n1-1, n2-1, false);
					
					y = pqT_pq.keySet().iterator().next();

					S.put(ps,1);
					CS.putAll(pqT_pq);
					StoCS.put(ps, y);


				}
				else{


					if(S.get(ps)==1){

						pqT_pq.put(StoCS.get(ps),CS.get(StoCS.get(ps)));
						
					

					}
					else{

						
						pqT_pq = explore("", "", y, p.asResource(), pT_p.get(p), q.asResource(), qT_q.get(q), n1-1, n2-1, true);//, endpoint_service);
						
						y = pqT_pq.keySet().iterator().next();
						S.put(ps,1);
						CS.putAll(pqT_pq);	

		
					}

				}
			


				RDFNode z = ResourceFactory.createResource();
				HashMap<Resource,List<ArrayList<RDFNode>>> dT_d = new HashMap<Resource,List<ArrayList<RDFNode>>>();
				HashMap<Resource,List<ArrayList<RDFNode>>> cdT_cd = new HashMap<Resource,List<ArrayList<RDFNode>>>();

				if(!c.isLiteral()){

					if(!d.isLiteral()){

						triple_b++;
						Set<Resource> sd = new HashSet<Resource>();
						if(n2-1>0){
							long sigma_bS = System.currentTimeMillis();
							if(!d.isAnon()){
								
								stat_r2 = stat_r2 + " <" + d + "> .";
								
								dT_d.putAll(compute_sigma(d.asResource(),n2-1));
								sd.addAll(dT_d.keySet());
							}
							else{
								
								stat_r2 = stat_r2 + " ?ob"+(n2-1)+" .";
								
								dT_d.putAll(compute_sigma_blank(blank_b+stat_r2,d.asResource(),n2-1));
								
								sd.addAll(dT_d.keySet());
							}
							long sigma_bE = System.currentTimeMillis();
							if(w==1)
								sigma_b = sigma_b + (sigma_bE - sigma_bS);
						}
						else{
							dT_d.put(d.asResource(), new ArrayList<ArrayList<RDFNode>>());
							sd.addAll(dT_d.keySet());
						}
						
						Iterator<Resource> isc = sc.iterator();
						while(isc.hasNext()){
							c = (RDFNode) isc.next();
							Iterator<Resource> isd = sd.iterator();
							while(isd.hasNext()){
						
								d = (RDFNode) isd.next();
								
								String[] stringo = {c.toString(), d.toString()};
								
								Arrays.sort(stringo, String.CASE_INSENSITIVE_ORDER);
								String os = stringo[0]+stringo[1];
								
								contained = false;

								if(S.containsKey(os)){
									contained = true;
									z = StoCS.get(os);
								}
								
								if(!contained){	
									cdT_cd = explore(blank_a+stat_r1, blank_b+stat_r2, z.asResource(), c.asResource(), cT_c.get(c), d.asResource(), dT_d.get(d), n1-1, n2-1, false);
									
									z =  cdT_cd.keySet().iterator().next(); 
									
									

									S.put(os,1);
									CS.putAll(cdT_cd);
									StoCS.put(os, z.asResource());

								}
								else{ 

									if(S.get(os)==1){
										cdT_cd.put(StoCS.get(os),CS.get(StoCS.get(os)));
										
									}
									else{
										cdT_cd = explore(blank_a+stat_r1, blank_b+stat_r2, z.asResource(), c.asResource(), cT_c.get(c), d.asResource(), dT_d.get(d), n1-1, n2-1, true);//, endpoint_service);
										z =  cdT_cd.keySet().iterator().next(); 
										
										
										S.put(os, 1);
										CS.putAll(cdT_cd);

									}

									
								}
								
								ArrayList<RDFNode> yz = new ArrayList<RDFNode>();
								if(pqT_pq.get(y).size()!=0 || !y.isAnon() || cdT_cd.get(z.asResource()).size()!=0 || !z.isAnon()){
									yz.add(y);
									yz.add(z);
									T_x.add(yz);
								}
	
							}
						}
					}
					else{	
						cdT_cd.put(z.asResource(),new ArrayList<ArrayList<RDFNode>>());
						CS.put(z.asResource(),new ArrayList<ArrayList<RDFNode>>());

					}

				}
				else{

					if(d.isLiteral()){

						if(c.toString().equals(d.toString()))
							z = c;
						else{
							cdT_cd.put(z.asResource(),new ArrayList<ArrayList<RDFNode>>());
							CS.put(z.asResource(),new ArrayList<ArrayList<RDFNode>>());
						}

					}
					else{
						cdT_cd.put(z.asResource(),new ArrayList<ArrayList<RDFNode>>());
						CS.put(z.asResource(),new ArrayList<ArrayList<RDFNode>>());

					}

				}


				ArrayList<RDFNode> yz = new ArrayList<RDFNode>();

				if(c.isLiteral() && d.isLiteral() && c.equals(d)){
					yz.add(y);
					yz.add(z);
					T_x.add(yz);
				}
				else{
					if(c.isLiteral() || d.isLiteral()){
						if(pqT_pq.get(y).size()!=0 || !y.isAnon() || cdT_cd.get(z.asResource()).size()!=0 || !z.isAnon()){
							yz.add(y);
							yz.add(z);
							T_x.add(yz);
						}
					}
				}



			}


		}

		xT_x.put(x, T_x);	

		return xT_x;


	}
	
public static int compileQuery(Resource x, int n_a, String a, String b){
		
		List<ArrayList<RDFNode>> listx = new ArrayList<ArrayList<RDFNode>>();
		listx = CS.get(x);
		System.out.println("n. CS: "+listx.size());
		
		int fn = 0;
	
		if(x.isAnon() && listx.size()!=0){

			
			HashMap<Resource,List<RDFNode>> list = processCS(listx);
			
			Iterator it = list.keySet().iterator();
			
			while(it.hasNext()){
				
				Resource p = (Resource) it.next();
					
				List<RDFNode> z = list.get(p);
				
				if(z.size()!=0){
					
					Iterator yz = z.iterator();
					while(yz.hasNext()){
						
						fn++;
						int n=0;
						HashMap<Resource,String> Svar = new HashMap<Resource,String>();
						
						RDFNode o = (RDFNode) yz.next();
						
				
						try{
							PrintStream query = new PrintStream(new FileOutputStream("target/lod/clusters/v1-CS_"+n_a+"_"+a+"_"+b+"_D"+endpoint_service.size()+"query_"+fn+".txt"),false);
		
							query.println("SELECT DISTINCT ?s0 WHERE{"+where);
							String vars = "?s"+n;
							Svar.put(x, vars);
							n++;
							
			
		
							if(!o.isLiteral()){
								if(!p.isAnon() && !o.isAnon()){
									query.println(vars+"\t<"+p+">\t<"+o+"> .");
								}
								if(!p.isAnon() && o.isAnon()){
									String varo = new String();
									if(Svar.keySet().contains(o))
										varo = Svar.get(o);
									else{
										varo = "?s"+n;
										Svar.put((Resource) o, varo);
										n++;
										if(CS.keySet().contains(o)){
											writeQuery(query, o, n, Svar);
										}
									
									}
									query.println(vars+"\t<"+p+">\t"+varo+" .");
									
										
								}
								if(p.isAnon() && !o.isAnon()){
									String varp = new String();
									if(Svar.keySet().contains(p))
										varp = Svar.get(p);
									else{
										varp = "?s"+n;
										Svar.put(p, varp);
										n++;
										if(CS.keySet().contains(p))
											writeQuery(query, p, n, Svar);
									}
									query.println(vars+"\t"+varp+"\t<"+o+"> .");
									
								}
								if(p.isAnon() && o.isAnon()){
									String varp = new String();
									String varo = new String();
									if(Svar.keySet().contains(p))
										varp = Svar.get(p);
									else{
										varp = "?s"+n;
										Svar.put(p, varp);
										n++;
										if(CS.keySet().contains(p))
											n = writeQuery(query, p, n, Svar);
									}
									if(Svar.keySet().contains(o))
										varo = Svar.get(o);
									else{
										varo = "?s"+n;
										Svar.put((Resource) o, varo);
										n++;
										if(CS.keySet().contains(o))
											writeQuery(query, o, n, Svar);
									}
									query.println(vars+"\t"+varp+"\t"+varo+" .");
									
									
								}
							}
							else{
								String ogg = o.toString();
								int iat = ogg.lastIndexOf("@");
								String pre = new String();
								String post = new String();
								if(iat>-1){
									pre = ogg.substring(0, iat);
									post = ogg.substring(iat, ogg.length());
								}
								
								if(!p.isAnon()){
									if(iat>-1){
										query.println(vars+"\t<"+p+">\t\""+pre+"\""+post+" .");//+"\t"+yz.get(1));
									}
									else{
										query.println(vars+"\t<"+p+">\t\""+o+"\" .");//+"\t"+yz.get(1));
									}
								}
								else{
									String varp = new String();
									if(Svar.keySet().contains(p))
										varp = Svar.get(p);
									else{
										varp = "?s"+n;
										Svar.put(p, varp);
										n++;
										if(CS.keySet().contains(p))
											writeQuery(query, p, n, Svar);
									}
									if(iat>-1){
										query.println(vars+"\t"+varp+"\t\""+pre+"\""+post+" .");
									}
									else{
										query.println(vars+"\t"+varp+"\t\""+o+"\" .");
									}
								
								}

							}
							
							query.println("}");
							query.close();


						}catch(Exception e){

							System.out.println("errore scrittura su file "+fn+": "+e);

						}
					}
				}
				else{



					if(!p.isAnon() || CS.get(p).size()!=0){
					
						fn++;
						int n=0;
						HashMap<Resource,String> Svar = new HashMap<Resource,String>();


						try{
							PrintStream query = new PrintStream(new FileOutputStream("target/lod/clusters/v1-CS_"+n_a+"_"+a+"_"+b+"_D"+endpoint_service.size()+"query_"+fn+".txt"),false);
						
							query.println("SELECT DISTINCT ?s0 WHERE{"+where);
							String vars = "?s"+n;
							Svar.put(x, vars);
							n++;
							

							if(!p.isAnon()){
								query.println(vars+"\t<"+p+">\t?s"+n+" .");	
							}
							else{
								String varp = new String();
								if(Svar.keySet().contains(p))
									varp = Svar.get(p);
								else{
									varp = "?s"+n;
									Svar.put(p, varp);
									n++;
									
									
									n = writeQuery(query, p, n, Svar);
								}
								
								query.println(vars+"\t"+varp+"\t?s"+n+" .");
								n++;
								
							}

							query.println("}");
							query.close();


						}catch(Exception e){

							System.out.println("errore scrittura su file "+fn+": "+e);

						}
					}

				}

			}
		}
		
		return fn;
	}

public static ArrayList<Resource> executeQueries(int fn, int n_a, String a, String b) {
	// TODO Auto-generated method stub
	ArrayList<Resource> r = new ArrayList<Resource>();
	ArrayList<String> q = new ArrayList<String>();
	
	System.out.println("query in ingresso: "+fn);
	
	for(int i=1; i<=fn; i++){
		 try {
			BufferedReader br = new BufferedReader(new FileReader("target/lod/clusters/v1-CS_"+n_a+"_"+a+"_"+b+"_D"+endpoint_service.size()+"query_"+i+".txt"));
			StringBuffer sb = new StringBuffer();
			String querystring;
			try {
				while((querystring = br.readLine()) != null)
					sb.append(querystring+" ");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				
				
				if(!q.contains(sb.toString())){
					q.add(sb.toString());
					ArrayList<Resource> r1 = doQuery(sb.toString());
					
					if(i>1){
						r1.retainAll(r);
						r = r1;
					}
					else{
						r = r1;
					}
					
				}
				
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	System.out.println("query in uscita: "+q.size());
	
	
	return r;

}

public static HashMap<Resource,List<ArrayList<RDFNode>>> compute_sigma_blank(String blank_string, Resource resource, int n_r){//, String endpoint_service){

	HashMap<Resource,List<ArrayList<RDFNode>>> rgraph = new HashMap<Resource,List<ArrayList<RDFNode>>>();
	List<ArrayList<RDFNode>> T_r = new ArrayList<ArrayList<RDFNode>>();

	if(n_r>0){

		String s = "?ob"+n_r;
		String p = "?p";
		String o = "?o";

		String querystring =
				
				"SELECT DISTINCT "+s+" "+p+" "+o+" \n" +
				"WHERE\n" +
				"  { " +
				blank_string + 
				
				" "+s+" "+p+" "+o+" . \n" ;
		
		
		
		querystring = querystring +
				"FILTER(isBlank("+s+")) \n" +
				"FILTER(?p != <http://dbpedia.org/ontology/wikiPageWikiLink>) \n" +
				"FILTER(?p != <http://purl.org/dc/elements/1.1/title>) \n" +
				"FILTER(?p != <http://dati.camera.it/ocd/endDate>) \n" +
				"FILTER(?p != <http://dati.camera.it/ocd/startDate>) \n" +
				"FILTER(?p != <http://purl.org/dc/elements/1.1/date>) \n" +
				"FILTER(?p != <http://dati.camera.it/ocd/file>) \n" +
				"FILTER(?p != <http://www.w3.org/2000/01/rdf-schema#label>) \n" +
				"FILTER(?p != <http://purl.org/dc/terms/isReferencedBy>) \n" +
				"FILTER(?p != <http://lod.xdams.org/ontologies/ods/modified>) \n" +
				"FILTER(?p != <http://xmlns.com/foaf/0.1/nick>) \n" +
				"FILTER(?p != <http://xmlns.com/foaf/0.1/depiction>) \n" +
				"FILTER(?p != <http://xmlns.com/foaf/0.1/firstName>) \n" +
				"FILTER(?p != <http://xmlns.com/foaf/0.1/surname>) \n" +
				"FILTER(?p != <http://www.w3.org/2000/01/rdf-schema#comment>) \n" +
				"  } \n" +
				
				"";

		

		Query query = QueryFactory.create(querystring);
		List<QuerySolution> answer = new ArrayList<QuerySolution>();

		Iterator<String> es = endpoint_service.iterator();
		while(es.hasNext()){
			QueryExecution x = QueryExecutionFactory.sparqlService((String) es.next(), query);
			ResultSet results = x.execSelect();
			answer.addAll(ResultSetFormatter.toList(results));
			
		}
		
		//%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
		HashMap<Resource,Boolean> pb = new HashMap<Resource,Boolean>();
		Iterator<QuerySolution> ia = answer.iterator();
		Resource ans = ResourceFactory.createResource();
		if(answer.size()>0)
			ans = answer.get(0).getResource(s);
		while(ia.hasNext()){
			QuerySolution spo = (QuerySolution) ia.next();
			if(!ans.equals(spo.get(s))){
				Iterator<Resource> ipb = pb.keySet().iterator();
				while(ipb.hasNext()){

					ArrayList<RDFNode> po = new ArrayList<RDFNode>();
					Resource pbo = ipb.next();
					po.add(pbo);	
					po.add(ResourceFactory.createResource());	
					T_r.add(po);
					
				}
				rgraph.put(ans, T_r);
				T_r = new ArrayList<ArrayList<RDFNode>>();
				pb.clear();
				ans = spo.get(s).asResource();
				ArrayList<RDFNode> po = new ArrayList<RDFNode>();
				if(spo.get(p).isResource() && !spo.get(p).isAnon()){
					
					if(spo.get(o).isResource() && !spo.get(o).isAnon()){
						po.add(spo.get(p).asResource());
						po.add(spo.getResource(o));
						T_r.add(po);	
					}
					else{
						if(spo.get(o).isAnon()){
							pb.put(spo.get(p).asResource(), Boolean.TRUE);
						}
						else{
							po.add(spo.getResource(p));
							po.add(spo.getLiteral(o));
							T_r.add(po);	
						}
					}
					
				}	
			}
			else{
				ArrayList<RDFNode> po = new ArrayList<RDFNode>();
				if(spo.get(p).isResource() && !spo.get(p).isAnon()){
					
					if(spo.get(o).isResource()&& !spo.get(o).isAnon()){
						po.add(spo.get(p).asResource());
						po.add(spo.getResource(o));
						T_r.add(po);
					}
					else{
						if(spo.get(o).isAnon()){
							pb.put(spo.get(p).asResource(), Boolean.TRUE);
						}
						else{
							po.add(spo.get(p).asResource());
							po.add(spo.getLiteral(o));	
							T_r.add(po);
						}
					}
					
				}	
			}			
		}
		Iterator<Resource> ipb = pb.keySet().iterator();
		while(ipb.hasNext()){

			ArrayList<RDFNode> po = new ArrayList<RDFNode>();
			Resource pbo = ipb.next();
			po.add(pbo);	
			po.add(ResourceFactory.createResource());	
			T_r.add(po);
			
		}
		rgraph.put(ans, T_r);
		
		
		
	}

	rgraph.put(resource, T_r);
	return rgraph;

}

public static HashMap<Resource,List<RDFNode>> processCS(List<ArrayList<RDFNode>> listx){

	HashMap<Resource,List<RDFNode>> polist = new HashMap<Resource,List<RDFNode>>();
	Iterator it = listx.iterator();
	while(it.hasNext()){
		List<RDFNode> objects = new ArrayList<RDFNode>();
		Set<Resource> pred = polist.keySet();
		List<RDFNode> po = (List<RDFNode>) it.next();
		Resource p = (Resource) po.get(0);
		RDFNode o = po.get(1);
		if(!pred.contains(p)){
			polist.put(p, objects);
		}
		else{
			objects.addAll(polist.get(p));
		}
		if(!o.isLiteral()){
			if(!o.isAnon()){
				if(!objects.contains(o))
					objects.add(o);
			}
			else{
				if(!objects.contains(o) && CS.get(o).size()!=0){
					objects.add(o);
				}
			}
		}
		else{
			objects.add(o);
		}
		polist.put(p, objects);
	}

	return polist;
}

public static int writeQuery(PrintStream query, RDFNode x, int n, HashMap<Resource,String> Svar){//, int n_a){//, String a,String b){

	List<ArrayList<RDFNode>> listx = new ArrayList<ArrayList<RDFNode>>();
	HashMap<Resource,List<RDFNode>> list = new HashMap<Resource,List<RDFNode>>();

	listx = CS.get(x);


	if(listx.size()!=0){

		list = processCS(listx);

		Iterator it = list.keySet().iterator();

		

		while(it.hasNext()){

			Resource p = (Resource) it.next();

			String vars = Svar.get(x);
			

			List<RDFNode> yz = list.get(p);

			if(yz.size()!=0){
				Iterator iyz = yz.iterator();
				while(iyz.hasNext()){
					RDFNode o = (RDFNode) iyz.next();
					if(!o.isLiteral()){
						if(!p.isAnon() && !o.isAnon()){
							query.println(vars+"\t<"+p+">\t<"+o+"> .");
						}
						if(!p.isAnon() && o.isAnon()){
							String varo = new String();
							if(Svar.keySet().contains(o))
								varo = Svar.get(o);
							else{
								varo = "?s"+n;
								Svar.put((Resource) o, varo);
								n++;
								if(CS.keySet().contains(o))
									n = writeQuery(query, o, n, Svar);
							}
							query.println(vars+"\t<"+p+">\t"+varo+" .");
							
						}
						if(p.isAnon() && !o.isAnon()){
							String varp = new String();
							if(Svar.keySet().contains(p))
								varp = Svar.get(p);
							else{
								varp = "?s"+n;
								Svar.put(p, varp);
								n++;
								if(CS.keySet().contains(p))
									n = writeQuery(query, p, n, Svar);
							}
							query.println(vars+"\t"+varp+"\t<"+o+"> .");
							
						}
						if(p.isAnon() && o.isAnon()){
							String varp = new String();
							String varo = new String();
							if(Svar.keySet().contains(p))
								varp = Svar.get(p);
							else{
								varp = "?s"+n;
								Svar.put(p, varp);
								n++;
								if(CS.keySet().contains(p))
									n = writeQuery(query, p, n, Svar);
							}
							if(Svar.keySet().contains(o))
								varo = Svar.get(o);
							else{
								varo = "?s"+n;
								Svar.put((Resource) o, varo);
								n++;
								if(CS.keySet().contains(o))
									n = writeQuery(query, o, n, Svar);
							}
							query.println(vars+"\t"+varp+"\t"+varo+" .");
							
							
						}
					}
					else{
						String ogg = o.toString();
						int iat = ogg.lastIndexOf("@");
						String pre = new String();
						String post = new String();
						if(iat>-1){
							pre = ogg.substring(0, iat);
							post = ogg.substring(iat, ogg.length());
						}
						if(!p.isAnon()){
							
							if(iat>-1){
								query.println(vars+"\t<"+p+">\t\""+pre+"\""+post+" .");
							}
							else{
								query.println(vars+"\t<"+p+">\t\""+o+"\" .");
							}

						}
						else{
							String varp = new String();
							if(Svar.keySet().contains(p))
								varp = Svar.get(p);
							else{
								varp = "?s"+n;
								Svar.put(p, varp);
								n++;
								if(CS.keySet().contains(p))
									n = writeQuery(query, p, n, Svar);
							}
							if(iat>-1){
								query.println(vars+"\t"+varp+"\t\""+pre+"\""+post+" .");
							}
							else{
								query.println(vars+"\t"+varp+"\t\""+o+"\" .");
							}
							query.println(vars+"\t"+varp+"\t\""+o+"\" .");
							
						}

					}

				}
			}
			else{
				if(!p.isAnon()){
					query.println(vars+"\t<"+p+">\t?s"+n+" .");
					n++;
				}
				else{
					String varp = new String();
					if(Svar.keySet().contains(p))
						varp = Svar.get(p);
					else{
						varp = "?s"+n;
						Svar.put(p, varp);
						n++;
						if(CS.keySet().contains(p))
							n = writeQuery(query, p,n, Svar);
					}
					query.println(vars+"\t"+varp+"\t?s"+n+" .");
					n++;
					
				}

			}

		}

	}


	return n;

}












}
