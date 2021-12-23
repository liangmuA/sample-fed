package com.lq;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import java.util.HashMap;
import java.util.Map;

public class Utils {

    public static int getTotalTriples(RepositoryConnection connection) {
        int totalTriples = 0;

        TupleQuery tq = connection.prepareTupleQuery("select (count(*) as ?triples) where { ?s ?p ?o }");

        try(TupleQueryResult tqRes = tq.evaluate()) {
            while (tqRes.hasNext()) {
                BindingSet next = tqRes.next();
                totalTriples += Integer.parseInt(next.getValue("triples").stringValue());
            }
        }

        return totalTriples;
    }

    public static int getDistinctSubject(RepositoryConnection connection) {
        int count = 0;

        TupleQuery tq = connection.prepareTupleQuery("select (count(distinct ?s) as ?count) where { ?s ?p ?o }");
        tq.setMaxExecutionTime(600);

        while (count == 0) {
            try {
                TupleQueryResult tqRes = tq.evaluate();
                BindingSet next = tqRes.next();
                count += Integer.parseInt(next.getValue("count").stringValue());

                tqRes.close();
            } catch (Exception e) {}
        }


        return count;
    }

    public static int getDistinctObject(RepositoryConnection connection) {
        int count = 0;

        TupleQuery tq = connection.prepareTupleQuery("select (count(distinct ?o) as ?count) where { ?s ?p ?o filter(isIRI(?o)) }");
        tq.setMaxExecutionTime(600);

        TupleQueryResult tqRes = tq.evaluate();
        BindingSet next = tqRes.next();
        count += Integer.parseInt(next.getValue("count").stringValue());

        tqRes.close();

        return count;
    }

    public static Map<String, Integer> dsSbj = new HashMap<>();
    public static Map<String, Integer> dsObj = new HashMap<>();
    public static Map<String, Integer> triples = new HashMap<>();
    public static Map<String, Integer> predicates = new HashMap<>();

    static {
        dsSbj.put("chebi", 50477);
        dsSbj.put("dbpedia", 9495865);
        dsSbj.put("drugbank", 19693);
        dsSbj.put("geonames", 7479714);
        dsSbj.put("jamendo", 335925);
        dsSbj.put("kegg", 34260);
        dsSbj.put("mdb", 694400);
        dsSbj.put("nty", 21666);
        dsSbj.put("swdf", 11974);
    }

    static {
        dsObj.put("chebi", 772138);
        dsObj.put("dbpedia", 13620028);
        dsObj.put("drugbank", 276142);
        dsObj.put("geonames", 35799392);
        dsObj.put("jamendo", 440686);
        dsObj.put("kegg", 939258);
        dsObj.put("mdb", 2052959);
        dsObj.put("nty", 191538);
        dsObj.put("swdf", 37547);
    }

    static {
        triples.put("chebi", 4778904);
        triples.put("dbpedia", 42855361);
        triples.put("drugbank", 522775);
        triples.put("geonames", 107955837);
        triples.put("jamendo", 1055399);
        triples.put("kegg", 1096582);
        triples.put("mdb", 6153748);
        triples.put("nty", 340950);
        triples.put("swdf", 109347);
    }

    static {
        predicates.put("chebi", 140);
        predicates.put("dbpedia", 1169);
        predicates.put("drugbank", 230);
        predicates.put("geonames", 137);
        predicates.put("jamendo", 138);
        predicates.put("kegg", 131);
        predicates.put("mdb", 333);
        predicates.put("nty", 146);
        predicates.put("swdf", 220);
    }

}
