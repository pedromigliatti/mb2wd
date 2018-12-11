
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.*;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.ext.com.google.common.cache.Cache;
import org.apache.jena.ext.com.google.common.cache.CacheBuilder;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;


public class Main {
    private final static int DUP_WINDOW = 1000;

    public static void main(String[] args) throws IOException, NotFoundException {
        boolean streamMode = false;

        HDT hdtMusicbrainz = HDTManager.mapIndexedHDT("index_big_musicbrainz.hdt", null);
        HDT hdtWikidata = HDTManager.mapIndexedHDT("index_big.hdt", null);

        File file = new File("output.rdf");
        FileOutputStream fop = new FileOutputStream(file);
        StreamRDF writer = StreamRDFWriter.getWriterStream(fop, Lang.NTRIPLES);

        // Create Jena wrapper on top of HDT.
        HDTGraph graphMusicbrainz = new HDTGraph(hdtMusicbrainz);
        Model modelMusicbrainz = ModelFactory.createModelForGraph((Graph) graphMusicbrainz);


        HDTGraph graphWikidata = new HDTGraph(hdtWikidata);
        Model modelWikidata = ModelFactory.createModelForGraph((Graph) graphWikidata);

        // Use Jena ARQ to execute the query.
        String sparqlQuery = "select * where { \n" +
                "?s <http://xmlns.com/foaf/0.1/isPrimaryTopicOf> ?o .\n" +
                "FILTER ( regex(str(?o), \"^https://www.wikidata.org/wiki/\"))\n" +
                " } ";

        Query queryMusicbrainz = QueryFactory.create(sparqlQuery);
        QueryExecution qem = QueryExecutionFactory.create(queryMusicbrainz, modelMusicbrainz);


//        MusicBrainz work ID (P435)
//        MusicBrainz label ID (P966)
//        MusicBrainz artist ID (P434)
//        MusicBrainz release group ID (P436)
//        MusicBrainz area ID (P982)
//        MusicBrainz place ID (P1004)
//        MusicBrainz recording ID (P4404)
//        MusicBrainz instrument ID (P1330)
//        MusicBrainz series ID (P1407)
//        MusicBrainz release ID (P5813)

        HashMap<String, String> wikidataProperties = new HashMap<String, String>();
        wikidataProperties.put("P434","artist");
        wikidataProperties.put("P435","work");
        wikidataProperties.put("P966","label");
        wikidataProperties.put("P436","release_group");
        wikidataProperties.put("P382","area");
        wikidataProperties.put("P1004","place");
        wikidataProperties.put("P4404","recording");
        wikidataProperties.put("P1330","instrument");
        wikidataProperties.put("P1407","series");
        wikidataProperties.put("P5813","release");

        sparqlQuery = "select * where { \n" +
                "?s <http://www.wikidata.org/prop/direct/P434> ?artist .\n" +
                " } ";

        Query queryWikidata = QueryFactory.create(sparqlQuery);
        QueryExecution qew = QueryExecutionFactory.create(queryWikidata, modelMusicbrainz);

        try {
            // Perform the query and output the results, depending on query type
            if (queryMusicbrainz.isSelectType()) {
                ResultSet results = qem.execSelect();
                Node predicate = NodeFactory.createURI("https://www.w3.org/OWL/sameAs");
                writer.start();
                while (results.hasNext()) {
                    QuerySolution result = results.next();
                    Node subject = NodeFactory.createURI(result.get("s").toString());
                    Node object = NodeFactory.createURI(result.get("o").toString());
                    Triple t = new Triple(subject, predicate, object);
                    writer.triple(t);
                }

                String lastProperty = "P434";

                for(HashMap.Entry<String,String> property: wikidataProperties.entrySet()){
                    sparqlQuery.replace(lastProperty, property.getKey());
                    sparqlQuery.replace(wikidataProperties.get(lastProperty), property.getValue());

                    queryWikidata = QueryFactory.create(sparqlQuery);
                    qew = QueryExecutionFactory.create(queryWikidata, modelMusicbrainz);

                    results = qew.execSelect();

                    while (results.hasNext()) {
                        QuerySolution result = results.next();
                        Node subject = NodeFactory.createURI("https://musicbrainz.org/" + property.getValue() + "/" +result.get(property.getValue()).toString());
                        Node object = NodeFactory.createURI(result.get("s").toString());
                        Triple t = new Triple(subject, predicate, object);
                        writer.triple(t);
                    }

                    lastProperty = property.getKey();
                }

                writer.finish();
            } else if (queryMusicbrainz.isDescribeType()) {
                if (streamMode) {
                    Iterator<Triple> results = qem.execDescribeTriples();
                    streamResults(results);
                } else {
                    Model result = qem.execDescribe();
                    result.write(System.out, "N-TRIPLES", null);
                }
            } else if (queryWikidata.isConstructType()) {
                if (streamMode) {
                    Iterator<Triple> results = qem.execConstructTriples();
                    streamResults(results);
                } else {
                    Model result = qem.execConstruct();
                    result.write(System.out, "N-TRIPLES", null);
                }
            } else if (queryWikidata.isAskType()) {
                boolean b = qem.execAsk();
                System.out.println(b);
            }
        } finally {
            qem.close();
        }
    }

    private static void streamResults(Iterator<Triple> results) {
        StreamRDF writer = StreamRDFWriter.getWriterStream(System.out, Lang.NTRIPLES);
        Cache<Triple, Boolean> seenTriples = CacheBuilder.newBuilder()
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
