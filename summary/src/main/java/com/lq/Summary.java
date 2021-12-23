package com.lq;

import com.alibaba.fastjson.JSONObject;
import com.lq.construct.Pair;
import com.lq.construct.Trie2;
import com.lq.construct.Tuple3;
import com.lq.construct.Tuple4;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Summary {

    private static final Logger log = LoggerFactory.getLogger(Summary.class);

    ExecutorService executorService;
    List<Future<?>> tasks = new ArrayList<Future<?>>();
    BufferedWriter bwr;

    static int MIN_TOP_OBJECTS = 10;
    static int MAX_TOP_OBJECTS = 100;

    public Summary(String location) throws IOException {
        File file = new File(location);

        if (!file.exists()) {
            File folder = new File(file.getParent());
            if (!folder.exists()) {
                folder.mkdirs();
            }
            file.createNewFile();
        }

        bwr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(location), StandardCharsets.UTF_8));
        bwr.append("@prefix ds:<http://aksw.org/quetsal/> .");
        bwr.newLine();
    }

    public void generateSummaries(List<String> endpoints, List<String> rdfPaths, int branchLimit, double[] tg) throws IOException {
        executorService = Executors.newFixedThreadPool(16);

        for (int i = 0; i < endpoints.size(); i++) {
            String endpoint = endpoints.get(i);
            String rdfPath = rdfPaths.get(i);

            int finalI = i;
            addTask(new Runnable() {
                @Override
                public void run() {
                    try {
                        String sum = generateSummary(endpoint, rdfPath, branchLimit, tg[finalI], GeneraSummary.originInfo.getJSONObject(finalI));
                        synchronized(bwr) {
                            try {
                                bwr.append(sum);
                                bwr.flush();
                            } catch (Exception e) {
                                log.error("", e);
                            }
                        }
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            });
        }

        waitForTasks();
        executorService.shutdown();
        bwr.close();
    }

    public String generateSummary(String endpoint, String rdfPath, int branchLimit, double tg, JSONObject jsonObject) {
        Storage storage = new Storage(rdfPath);

        long totalTrpl = 0;
        log.info("getting distinct subject count");
        long totalSbj = getDistinctSubjectCount(storage);
        log.info("total distinct subjects: "+ totalSbj + " for endpoint: " + endpoint);
        long totalObj = getDistinctObjectCount(storage);
        log.info("total distinct objects: "+ totalObj + " for endpoint: " + endpoint);
        List<String> listPred = getPredicates(storage);
        log.info("total distinct predicates: "+ listPred.size() + " for endpoint: " + endpoint);

        StringBuilder sb = new StringBuilder();

        sb.append("#---------------------").append(endpoint).append(" Summaries-------------------------------\n");
        sb.append("[] a ds:Service ;\n");
        sb.append("     ds:url   <").append(endpoint).append("> ;\n");

        List<Future<Tuple4<String, Long, Long, Long>>> subtasks = new ArrayList<Future<Tuple4<String, Long, Long, Long>>>();

        for (int i = 0; i < listPred.size(); i++) {
            String predicate = listPred.get(i);

            if (!verifyIRI(predicate)) {
                subtasks.add(null);
                continue;
            }

            Future<Tuple4<String, Long, Long, Long>> statfuture = addTask(new Callable<Tuple4<String, Long, Long, Long>>() {
                @Override
                public Tuple4<String, Long, Long, Long> call() throws Exception {
                    return writePrefixes(predicate, endpoint, storage, branchLimit, tg);
                }
            });
            subtasks.add(statfuture);
        }


        for (int i = 0; i < listPred.size(); i++) {
            if (subtasks.get(i) == null) {
                continue;
            }
            String predicate = listPred.get(i);

//            double d = jsonObject.getJSONObject("predicates").getJSONObject(predicate).getDouble("totalTriples")/(jsonObject.getDouble("totalTriples")/jsonObject.getJSONObject("predicates").size());

            StringBuilder tsb = new StringBuilder();

            try {
                tsb.append("     ds:capability\n");
                tsb.append("         [\n");
                tsb.append("           ds:predicate  <").append(predicate).append("> ;");

                Tuple4<String, Long, Long, Long> stat = subtasks.get(i).get();
                log.info("{} from {} done: {}, endpoint: {}", i+1, listPred.size(), predicate, endpoint);

                tsb.append(stat.getValue0());
                long distinctSbj = (long) (stat.getValue1()/tg*100* stat.getValue1()/ stat.getValue3());
                long distinctObj = (long) (stat.getValue2()/tg*100* stat.getValue2()/ stat.getValue3());
                long tripleCount = (long) (stat.getValue3()/tg*100);

                tsb.append("           ds:distinctSbjs ").append(distinctSbj).append(" ;\n");
                tsb.append("           ds:distinctObjs  ").append(distinctObj).append(" ;\n");
                tsb.append("           ds:triples    ").append(tripleCount).append(" ;\n");
                tsb.append("         ] ;\n");
                sb.append(tsb.toString());
                totalTrpl += tripleCount;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        sb.append("     ds:totalSbj ").append((long) (totalSbj/tg*totalSbj/totalTrpl)).append(" ; \n");  // this is not representing the actual number of distinct subjects in a datasets since the same subject URI can be shaared by more than one predicate. but we keep this to save record.
        sb.append("     ds:totalObj ").append((long) (totalObj/tg*totalSbj/totalTrpl)).append(" ; \n");
        sb.append("     ds:totalTriples ").append(totalTrpl).append(" ; \n");
        sb.append("             .\n");

        return sb.toString();
    }

    public Tuple4<String, Long, Long, Long> writePrefixes(String predicate, String endpoint, Storage storage, int branchLimit, double tg) {
        StringBuilder sb = new StringBuilder();

        long limit = 1000000;

        boolean isTypePredicate = predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        long rsCount = 0;
        long uniqueSbj = 0;
        long uniqueObj = 0;

        Trie2.Node rootSbj = Trie2.initializeTrie();
        Trie2.Node rootObj = Trie2.initializeTrie();

        long top = 0;

        long rc;
        do {
            rc = 0;

            RepositoryConnection conn = storage.getConnection();

            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL,
                    "SELECT ?s (count(?o) as ?oc) WHERE { " +
                            "?s <" + predicate + "> ?o. " +
                            "} GROUP BY ?s LIMIT " + limit + " OFFSET " + top);

            try(TupleQueryResult res = query.evaluate()) {
                while (res.hasNext()) {
                    ++rc;
                    BindingSet bs = res.next();
                    Value curSbj = bs.getValue("s");
                    ++uniqueSbj;
                    if (verifyIRI(curSbj)) {
                        putIRI(curSbj.stringValue(), Long.parseLong(bs.getValue("oc").stringValue()), rootSbj);
                    }
                }
            }

            top += rc;
        } while (rc == limit);

        top = 0;

        do {
            rc = 0;

            RepositoryConnection conn = storage.getConnection();

            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL,
                    "SELECT ?o (count(?s) as ?sc) WHERE { " +
                    "?s <" + predicate + "> ?o. " +
                    "} GROUP BY ?o LIMIT " + limit + " OFFSET " + top);

            try(TupleQueryResult res = query.evaluate()) {
                while (res.hasNext()) {
                    ++rc;
                    BindingSet bs = res.next();
                    Value curObj = bs.getValue("o");
                    ++uniqueObj;
                    if (verifyIRI(curObj)) {
                        putIRI(curObj.stringValue(), Long.parseLong(bs.getValue("sc").stringValue()), rootObj);
                    }
                }
            }

            top += rc;
        } while (rc == limit);

        rsCount = getTripleCount(predicate, storage);

        boolean first = true;
        sb.append("\n           ds:topSbjs");
        List<Pair<String, Long>> topsbjstotal = Trie2.findMostHittable(rootSbj, Math.max(1000, MAX_TOP_OBJECTS));
        List<Pair<String, Long>> topsbjs = new ArrayList<Pair<String, Long>>();
        List<String> middlesbjs = new ArrayList<String>();

        long avrmiddlesbjcard = doSaleemAlgo(MIN_TOP_OBJECTS, MAX_TOP_OBJECTS, topsbjstotal, topsbjs, middlesbjs);

        for (Pair<String, Long> p : topsbjs) {
            if (!first) sb.append(","); else first = false;
            sb.append("\n             [ ds:subject <").append(makeWellFormed(p.getFirst())).append(">; ds:card ").append(p.getSecond()).append(" ]");
        }

        if (!middlesbjs.isEmpty()) {
            sb.append(",\n             [ ds:middle ");
            first = true;
            for (String ms : middlesbjs) {
                if (!first) sb.append(","); else first = false;
                sb.append('<').append(makeWellFormed(ms)).append('>');
            }
            sb.append("; ds:card ").append(avrmiddlesbjcard).append(" ]");
        }

        if (topsbjs.isEmpty())
            sb.append(" []");

        first = true;
        sb.append(";\n           ds:topObjs");

        List<Pair<String, Long>> topobjstotal = Trie2.findMostHittable(rootObj, isTypePredicate ? Integer.MAX_VALUE : (Math.max(1000, MAX_TOP_OBJECTS)));
        List<Pair<String, Long>> topobjs = null;
        List<String> middleobjs = new ArrayList<String>();
        long avrmiddleobjcard = 0;
        if (!isTypePredicate) {
            topobjs = new ArrayList<Pair<String, Long>>();
            avrmiddleobjcard = doSaleemAlgo(MIN_TOP_OBJECTS, MAX_TOP_OBJECTS, topobjstotal, topobjs, middleobjs);
        } else {
            topobjs = topobjstotal;
        }

        for (Pair<String, Long> p : topobjs) {
            if (!first) sb.append(","); else first = false;
            sb.append("\n             [ ds:object <").append(makeWellFormed(p.getFirst())).append(">; ds:card ").append(p.getSecond()).append(" ]");
        }
        if (!middleobjs.isEmpty()) {
            sb.append(",\n             [ ds:middle ");
            first = true;
            for (String ms : middleobjs) {
                if (!first) sb.append(","); else first = false;
                sb.append('<').append(makeWellFormed(ms)).append('>');
            }
            sb.append("; ds:card ").append(avrmiddleobjcard).append(" ]");
        }
        if (topobjs.isEmpty())
            sb.append(" []");

        first = true;
        sb.append(";\n           ds:subjPrefixes");
        List<Tuple3<String, Long, Long>> sprefs = Trie2.gatherPrefixes(rootSbj, branchLimit);
        for (Tuple3<String, Long, Long> t : sprefs) {
            if (!first) sb.append(","); else first = false;
            sb.append("\n             [ ds:prefix \"").append(encodeStringLiteral(t.getValue0())).append("\"; ds:unique ").append((long) (t.getValue1()/tg*100)).append("; ds:card ").append((long) (t.getValue2()/tg*100)).append(" ]");
        }
        if (sprefs.isEmpty())
            sb.append(" []");

        first = true;
        sb.append(";\n           ds:objPrefixes");
        if (!isTypePredicate) {
            List<Tuple3<String, Long, Long>> oprefs = Trie2.gatherPrefixes(rootObj, branchLimit);
            for (Tuple3<String, Long, Long> t : oprefs) {
                if (!first) sb.append(","); else first = false;
                sb.append("\n             [ ds:prefix \"").append(encodeStringLiteral(t.getValue0())).append("\"; ds:unique ").append((long) (t.getValue1()/tg*100)).append("; ds:card ").append((long) (t.getValue2()/tg*100)).append(" ]");
            }
            if (oprefs.isEmpty()) sb.append(" []");
        } else {
            sb.append(" []");
        }

        sb.append(";\n");

        return new Tuple4<String, Long, Long, Long>(sb.toString(), uniqueSbj, uniqueObj, rsCount);
    }

    static String encodeStringLiteral(String s) {
        if (s.indexOf((int)'"') == -1) {
            return s;
        }
        return s.replace("\"", "\\\"");
    }

    static String makeWellFormed(String uri) {
        try {
            URL url = new URL(uri);
            return new URI(url.getProtocol(), url.getAuthority(), url.getPath(), url.getQuery(), url.getRef()).toURL().toString();
        } catch (Exception e) {
            log.error("Error in " + uri);
            throw new RuntimeException(e);
        }
    }

    Future<?> addTask(Runnable task) {
        Future<?> future = null;
        synchronized (executorService) {
            future = executorService.submit(task);
            tasks.add(future);
        }
        return future;
    }

    <T> Future<T> addTask(Callable<T> task) {
        Future<T> future = null;
        synchronized (executorService) {
            future = executorService.submit(task);
            tasks.add(future);
        }
        return future;
    }

    void waitForTasks() {
        Future<?> future = null;
        while (true) {
            synchronized (executorService) {
                if (tasks.isEmpty()) return;
                future = tasks.get(0);
            }
            try {
                future.get();
            } catch (Exception e) {
                log.error("", e);
            }
            synchronized (executorService) {
                tasks.remove(0);
            }
        }
    }

    long doSaleemAlgo(int min, int max, List<Pair<String, Long>> objects, List<Pair<String, Long>> top, List<String> middle) {
        while (!objects.isEmpty() && objects.get(objects.size() - 1).getSecond() == 1) {
            objects.remove(objects.size() - 1);
        }

        if (min > objects.size())
            min = objects.size();

        for (int i = 0; i < min; i++) {
            top.add(objects.get(i));
        }

        int max2 = Math.min(max, objects.size());

        // find first diff maximum
        long maxdiff = 0;
        int maxdiffidx = -1;
        for (int i = min; i < max2 - 1; ++i) {
            long diff = objects.get(i).getSecond() - objects.get(i + 1).getSecond();
            if (diff > maxdiff) {
                maxdiff = diff;
                maxdiffidx = i;
            }
        }
        if (maxdiffidx == -1)
            return 0;
        // copy objects
        for (int i = min; i < maxdiffidx; ++i) {
            Pair<String, Long> obj = objects.get(i);
            top.add(obj);
        }

        maxdiff = 0;
        int nextmaxdiffidx = -1;
        for (int i = maxdiffidx + 1; i < max2 - 1; ++i) {
            long diff = objects.get(i).getSecond() - objects.get(i + 1).getSecond();
            if (diff > maxdiff) {
                maxdiff = diff;
                nextmaxdiffidx = i;
            }
        }
        if (nextmaxdiffidx == -1) {
            nextmaxdiffidx = max2;
        }
        if (maxdiffidx == nextmaxdiffidx)
            return 0;

        long totalCard = 0;
        for (int i = maxdiffidx; i < nextmaxdiffidx; ++i) {
            Pair<String, Long> obj = objects.get(i);
            middle.add(obj.getFirst());
            totalCard += obj.getSecond();
        }

        return totalCard / (nextmaxdiffidx - maxdiffidx);
    }

    static Pair<String, String> splitIRI(String iri) {
        String[] objPrts = iri.split("/");
        String objAuth = objPrts[0] + "//" + objPrts[2];
        return new Pair<String, String>(objAuth, iri.substring(objAuth.length()));
    }

    boolean putIRI(String iri, long hits, Trie2.Node nd) {
        Pair<String, String> pair = splitIRI(iri);
        return Trie2.insertWord(nd, pair.getFirst(), pair.getSecond(), hits);
    }

    boolean verifyIRI(Value obj) {
        if (!(obj instanceof IRI)) return false;
        return verifyIRI(obj.stringValue());
    }

    public boolean verifyIRI(String sval) {
        try {
            // exclude virtuoso specific data
            if (sval.startsWith("http://www.openlinksw.com/schemas") || sval.startsWith("http://www.openlinksw.com/virtrd"))
                return false;
            URL url = new URL(sval);
            new URI(url.getProtocol(), url.getAuthority(), url.getPath(), url.getQuery(), url.getRef()).toURL().toString();
            return url.getProtocol() != null && url.getAuthority() != null && !url.getAuthority().isEmpty();
        } catch (Exception e) {
            //log.warn("skip IRI: " + sval + ", error: " + e.getMessage());
            return false;
        }
    }

    public Long getTripleCount(String pred, Storage storage) {
        long triples = 0L;

        TupleQuery query = storage.getConnection().prepareTupleQuery("select (count(*) as ?triples) where { ?s <" + pred +"> ?o } ");

        try(TupleQueryResult tqRes = query.evaluate()) {
            while (tqRes.hasNext()) {
                BindingSet next = tqRes.next();
                triples = Long.parseLong(next.getValue("triples").stringValue());
            }
        }

        return triples;
    }

    public List<String> getPredicates(Storage storage) {
        List<String> listPred = new ArrayList<>();

        TupleQuery query = storage.getConnection().prepareTupleQuery("select distinct ?p where { ?s ?p ?o . }");
        try (TupleQueryResult tqRes = query.evaluate()) {
            while (tqRes.hasNext()) {
                BindingSet next = tqRes.next();
                listPred.add(next.getBinding("p").getValue().stringValue());
            }
        }

        return listPred;
    }

    public long getDistinctObjectCount(Storage storage) {
        long totalObj = 0;

        TupleQuery query = storage.getConnection().prepareTupleQuery("select (count(distinct ?o) as ?count) where { ?s ?p ?o . }");
        try (TupleQueryResult tqRes = query.evaluate()) {
            while (tqRes.hasNext()) {
                BindingSet next = tqRes.next();
                totalObj = Long.parseLong(next.getBinding("count").getValue().stringValue());
            }
        }

        return totalObj;
    }

    public long getDistinctSubjectCount(Storage storage) {
        long totalSbj = 0;

        TupleQuery query = storage.getConnection().prepareTupleQuery("select (count(distinct ?s) as ?count) where { ?s ?p ?o . }");
        try (TupleQueryResult tqRes = query.evaluate()) {
            while (tqRes.hasNext()) {
                BindingSet next = tqRes.next();
                totalSbj = Long.parseLong(next.getBinding("count").getValue().stringValue());
            }
        }

        return totalSbj;
    }

}
