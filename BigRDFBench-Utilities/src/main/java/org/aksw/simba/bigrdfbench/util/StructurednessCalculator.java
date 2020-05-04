package org.aksw.simba.bigrdfbench.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.reasoner.rulesys.builtins.Print;

import javax.xml.transform.Result;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * Calculate the structuredness or coherence of a dataset as defined in Duan et al. paper titled "Apples and Oranges: A Comparison of
   RDF Benchmarks and Real RDF Datasets". The structuredness is measured in interval [0,1]  with values close to 0 corresponding to low structuredness, and
   1 corresponding to perfect structuredness. The paper concluded that synthetic datasets have high structurenes as compared to real datasets.
 * @author Saleem
 *
 */
public class StructurednessCalculator {

	public static void main(String[] args) throws IOException, URISyntaxException {
		if(args.length!=1 && args.length!=3  && args.length!=5){
			printHelp();
			return;
		}
		if(args[0].equals("-help")){
			printHelp();
			return;
		}
		String namedGraph=null;
		String endpointUrl = args[0];
		String output = null;
		if(args[0].equals("-named")){
			namedGraph = args[1];
			endpointUrl = args[2];
			if(args[2].equals("-file")){
				output = args[3];
				endpointUrl= args[4];
			}

		}else if(args[0].equals("-file")){
			 output = args[1];
			 endpointUrl = args[2];
			if(args[2].equals("-named")){
				namedGraph = args[3];
				endpointUrl= args[4];
			}
		}

		System.out.println("Endpoint:\t" + endpointUrl);
		System.out.println("Named Graph:\t" + namedGraph);


		double coherence = getStructurednessValue(endpointUrl, namedGraph, output);
		System.out.println("Structuredness:\t" + coherence);

	}

	private static void printHelp(){
		String helpText = "java -cp bigrdfbench.jar org.aksw.simba.bigrdfbench.util.StructurednessCalculator [-named <NAMED-GRAPH>] [-file <OUTPUT>] <ENDPOINT-URL>";
		System.out.println(helpText);
	}

	/**
	 * Get the structuredness/coherence value [0,1] of a dataset
	 * @param endpointUrl SPARQL endpoint URL
	 * @param namedGraph Named Graph of dataset. Can be null, in that case all named graphs will be considered
	 * @return structuredness Structuredness or coherence value
	 */
	public static double getStructurednessValue(String endpointUrl,
			String namedGraph, String output) throws IOException, URISyntaxException {
		StringBuilder writeStr = new StringBuilder();
		StringBuilder headerStr = new StringBuilder();

		Set<String> types = getRDFTypes(endpointUrl, namedGraph);
		System.out.println("Total rdf:types: " + types.size());
		headerStr.append("Total rdf:types\t");
		writeStr.append(types.size()).append("\t");

		double weightedDenomSum = getTypesWeightedDenomSum(endpointUrl, types,namedGraph);
		double structuredness = 0;
		long count = 1;
		for(String type:types)
		{
			long occurenceSum = 0;
			Set<String> typePredicates = getTypePredicates(endpointUrl, type,namedGraph);
			long typeInstancesSize = getTypeInstancesSize(endpointUrl, type,namedGraph);
			System.out.println(typeInstancesSize);
			System.out.println(type+" predicates: "+typePredicates);
			System.out.println(type+" : "+typeInstancesSize+" x " + typePredicates.size());
			for (String predicate:typePredicates)
			{
				long predicateOccurences = getOccurences(endpointUrl, predicate,type,namedGraph);
				occurenceSum = (occurenceSum + predicateOccurences);
				//System.out.println(predicate+ " occurences: "+predicateOccurences);
				//System.out.println(occurenceSum);
			}

			double denom = typePredicates.size()*typeInstancesSize;
			if(typePredicates.size()==0)
				denom = 1;
			//System.out.println("Occurence sum  = " + occurenceSum);
			//System.out.println("Denom = " + denom);
			double coverage = occurenceSum/denom;
			System.out.println("\n"+count+ " : Type: " + type );
			System.out.println("Coverage : "+ coverage);
			double weightedCoverage = (typePredicates.size()+ typeInstancesSize) / weightedDenomSum;
			System.out.println("Weighted Coverage : "+ weightedCoverage);
			structuredness = (structuredness + (coverage*weightedCoverage));
			count++;
		}
		headerStr.append("Structuredness");
		writeStr.append(structuredness);
		if(output!=null){
			try(PrintWriter pw = new PrintWriter(new FileOutputStream(
					new File(output),
					true /* append = true */))){
				pw.println(headerStr.toString());
				pw.println(writeStr.toString());
			}
		}
		return structuredness;
	}
	/**
	 * Get the denominator of weighted sum all types. Please see Duan et. all paper apple oranges
	 * @param types Set of rdf:types
	 * @param namedGraph Named graph
	 * @return sum Sum of weighted denominator
	 */
	public static double getTypesWeightedDenomSum(String endpoint, Set<String> types, String namedGraph) throws IOException, URISyntaxException {
		double sum = 0 ; 
		for(String type:types)
		{
			System.out.println("Current type: "+type);
			long typeInstancesSize = getTypeInstancesSize(endpoint, type,namedGraph);
			System.out.println("InstancesSize: "+typeInstancesSize);
			long typePredicatesSize = getTypePredicates(endpoint,type,namedGraph).size();
			System.out.println("typePredicatesSize: "+typeInstancesSize);
			sum = sum + typeInstancesSize + typePredicatesSize;

		}
		return sum;
	}
	/**
	 * Get occurences of a predicate within a type
	 * @param predicate Predicate
	 * @param type Type
	 * @param namedGraph Named Graph
	 * @return predicateOccurences Predicate occurence value
	 * @throws NumberFormatException
	 */
	public static long getOccurences(String endpoint, String predicate, String type, String namedGraph) throws NumberFormatException, IOException, URISyntaxException {
		long predicateOccurences = 0  ;
		String queryString ;
		if(namedGraph ==null)
			queryString = "SELECT (Count(Distinct ?s) as ?occurences) \n"
					+ "			WHERE { \n"
					+ "            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+"> . "
					+ "            ?s <"+predicate+"> ?o"
					+ "           }" ;
		else
			queryString = "SELECT (Count(Distinct ?s) as ?occurences) From <"+ namedGraph+"> \n"
					+ "			WHERE { \n"
					+ "            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+"> . "
					+ "            ?s <"+predicate+"> ?o"
					+ "           }" ;
		//System.out.println(queryString);
		ResultSet res = getQuery(endpoint, queryString);

		if(res.hasNext())
		{
			predicateOccurences = res.next().getLiteral("occurences").getLong();
		}
		return predicateOccurences;

	}
	/**
	 * Get the number of distinct instances of a specfici type
	 * @param type Type or class name
	 * @param namedGraph Named graph
	 * @return typeInstancesSize No of instances of type

	 */
	public static long getTypeInstancesSize(String endpoint, String type, String namedGraph) throws IOException, URISyntaxException {
		long typeInstancesSize =0;
		String queryString ;
		if(namedGraph ==null)
			queryString = "SELECT (Count(DISTINCT ?s)  as ?cnt)  \n"
					+ "			WHERE { \n"
					+ "            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+"> . "
					//+ "            ?s ?p ?o"
					+ "           }" ;
		else
			queryString = "SELECT (Count(DISTINCT ?s)  as ?cnt) From <"+ namedGraph+"> \n"
					+ "			WHERE { \n"
					+ "            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+"> . "
					//+ "            ?s ?p ?o"
					+ "           }" ;

		ResultSet res = getQuery(endpoint, queryString);

		if(res.hasNext())
		{
			typeInstancesSize =  res.next().getLiteral("cnt").getLong();
		}
		return typeInstancesSize;
	}

	private static ResultSet getQuery(String endpoint, String queryStr) throws IOException, URISyntaxException {
		CloseableHttpClient client = HttpClients
				.custom()
				.build();

		String encodedQuery = URLEncoder.encode(queryStr, "UTF-8");
		HttpUriRequest request = RequestBuilder.get()
				.setUri(endpoint+"?query="+encodedQuery)
				.setHeader(HttpHeaders.ACCEPT, "application/sparql-results+json")
				.build();
		HttpResponse response = client.execute(request);
		String responseStr =  EntityUtils.toString(response.getEntity());
		return ResultSetFactory.fromJSON(new ByteArrayInputStream(responseStr.getBytes()));

	}

	/**
	 * Get all distinct predicates of a specific type
	 * @param type Type of class
	 * @param namedGraph Named Graph can be null
	 * @return typePredicates Set of predicates of type

	 */
	public static Set<String> getTypePredicates(String endpoint, String type, String namedGraph) throws IOException, URISyntaxException {
		Set<String> typePredicates =new HashSet<String>() ;
		String queryString ;
		if(namedGraph ==null)
			queryString = "SELECT DISTINCT ?typePred \n"
					+ "			WHERE { \n"
					+ "            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+"> . "
					+ "            ?s ?typePred ?o"
					+ "           }" ;
		else
			queryString = "SELECT DISTINCT ?typePred From <"+ namedGraph+"> \n"
					+ "			WHERE { \n"
					+ "            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+"> . "
					+ "            ?s ?typePred ?o"
					+ "           }" ;
		//System.out.println(queryString);
		ResultSet res = getQuery(endpoint, queryString);

		while(res.hasNext())
		{
			String predicate = res.next().get("typePred").toString();
			if (!predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
				typePredicates.add(predicate);
		}
		return typePredicates;
	}

	/**
	 *  Get distinct set of rdf:type
	 * @param namedGraph Named Graph of dataset can be null in that case all namedgraphs will be considered
	 * @return types Set of rdf:types
	 */
	public static Set<String> getRDFTypes(String endpoint, String namedGraph) throws IOException, URISyntaxException {
		Set<String> types =new HashSet<String>() ;
		String queryString ="";
		if(namedGraph ==null)
			queryString = "SELECT DISTINCT ?type  "
					+ "			WHERE { "
					+ "            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type"
					+ "           }" ;
		else
			queryString = "SELECT DISTINCT ?type From <"+ namedGraph+"> "
					+ "			WHERE { "
					+ "            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type"
					+ "           }" ;
		//System.out.println(queryString);
		ResultSet res = getQuery(endpoint, queryString);

		while(res.hasNext())
		{
			types.add(res.next().get("type").toString());
		}
		return types;
	}
}
