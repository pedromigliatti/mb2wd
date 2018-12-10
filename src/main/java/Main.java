
import org.apache.jena.graph.Graph;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;


import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.ext.com.google.common.cache.Cache;
import org.apache.jena.ext.com.google.common.cache.CacheBuilder;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;


public class Main {
    private final static int DUP_WINDOW = 1000;

    public static void main(String[] args) throws IOException, NotFoundException {

        boolean streamMode = false;

        HDT hdt = HDTManager.mapIndexedHDT("/home/pedro/Documentos/mb2wd/index_big_musicbrainz.hdt", null);

        // Create Jena wrapper on top of HDT.
        HDTGraph graph = new HDTGraph(hdt);
        Model model = ModelFactory.createModelForGraph((Graph) graph);

        // Use Jena ARQ to execute the query.
        String sparqlQuery = "select * where { \n" +
                "?s <http://xmlns.com/foaf/0.1/isPrimaryTopicOf> ?o .\n" +
                "FILTER ( regex(str(?o), \"^https://www.wikidata.org/wiki/\"))\n" +
                " } ";

        Query query = QueryFactory.create(sparqlQuery);
        QueryExecution qe = QueryExecutionFactory.create(query, model);

        try {
            // Perform the query and output the results, depending on query type
            if (query.isSelectType()) {
                ResultSet results = qe.execSelect();
                ResultSetFormatter.outputAsCSV(System.out, results);
            } else if (query.isDescribeType()) {
                if (streamMode) {
                    Iterator<Triple> results = qe.execDescribeTriples();
                    streamResults(results);
                } else {
                    Model result = qe.execDescribe();
                    result.write(System.out, "N-TRIPLES", null);
                }
            } else if (query.isConstructType()) {
                if (streamMode) {
                    Iterator<Triple> results = qe.execConstructTriples();
                    streamResults(results);
                } else {
                    Model result = qe.execConstruct();
                    result.write(System.out, "N-TRIPLES", null);
                }
            } else if (query.isAskType()) {
                boolean b = qe.execAsk();
                System.out.println(b);
            }
        } finally {
            qe.close();
        }
    }

    private static void streamResults(Iterator<Triple> results) {
        StreamRDF writer = StreamRDFWriter.getWriterStream(System.out, Lang.NTRIPLES);
        Cache<Triple,Boolean> seenTriples = CacheBuilder.newBuilder()
                .maximumSize(DUP_WINDOW).build();

        writer.start();
        while (results.hasNext()) {
            Triple triple = results.next();
            if (seenTriples.getIfPresent(triple) != null) {
                // the triple has already been emitted
                continue;
            }
            seenTriples.put(triple, true);
            writer.triple(triple);
        }
        writer.finish();
    }
}
