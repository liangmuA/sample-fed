package com.lq;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Sample  {

    private static final Logger log = LoggerFactory.getLogger(Sample.class);

    private String method;
    private double size;
    private double ratio;
    private String name;
    private String endpoint;
    private String filePath;

    private Repository repository;
    private RepositoryConnection connection;
    private SampleWriter writer;

    public Sample(String method, double size, double ratio, String name, String endpoint, String filePath) {
        this.method = method;
        this.size = size;
        this.ratio = ratio;
        this.name = name;
        this.endpoint = endpoint;
        this.filePath = filePath;
    }

    public void init() {
        this.repository = new SPARQLRepository(endpoint);
        this.connection = repository.getConnection();
        this.writer = new SampleWriter(filePath);
    }

    public int execute() {
        long start = System.currentTimeMillis();

        int dsSbj = Utils.getDistinctSubject(connection);
        List<String> subjects = new ArrayList<>();

        if (method.equals("hybrid")) {
           int weightedSize = (int) (dsSbj * size * ratio);
           int unweightedSize = (int) (dsSbj * size * (1 - ratio));
           subjects.addAll(sampleSubject(true, weightedSize));
           subjects.addAll(sampleSubject(false, unweightedSize));
        } else {
            subjects.addAll(sampleSubject(method.equals("weighted"), (int) (dsSbj * size)));
        }

        log.info("{}, distinctSbj={}, size={}, subjects={}", connection.getRepository().toString(), dsSbj, size, subjects.size());

        int sampledTriples = sampleTriples(subjects);

        long end = System.currentTimeMillis();

        log.info("sampled triples={}, method={}, size={}, ratio={}, record={}s", sampledTriples, method, size, ratio, (end - start) / 1000);
        return sampledTriples;
    }

    public void close() {
        connection.close();
        repository.shutDown();
    }

    private int sampleTriples(List<String> subjects) {
        int cnt = 0;
        TupleQuery tq = null;
        TupleQueryResult tqRes = null;
        BindingSet bindingSet = null;

        for (int i = 0; i < subjects.size(); i += 10) {
            StringBuilder sb = new StringBuilder();
            sb.append("select");
            for (int j = 0; j < 10 && j + i < subjects.size(); j++) {
                sb.append(" ?p").append(j).append(" ?o").append(j);
            }
            sb.append(" where { ");
            for (int j = 0; j < 10 && j + i < subjects.size(); j++) {
                if (j != 0) {
                    sb.append(" union ");
                }
                sb.append("{ <").append(subjects.get(j+i)).append("> ?p").append(j).append(" ?o").append(j).append(" }");
            }
            sb.append(" }");

            tq = connection.prepareTupleQuery(sb.toString());

            try {
                tqRes = tq.evaluate();
                while (tqRes.hasNext()) {
                    bindingSet = tqRes.next();

                    for (int j = 0; j < 10 && j + i < subjects.size(); j++) {
                        if (bindingSet.getValue("p" + j) != null && bindingSet.getValue("o" + j) != null) {
                            cnt++;
                            writer.add(subjects.get(i + j), (IRI) bindingSet.getValue("p" + j), bindingSet.getValue("o" + j));
                            break;
                        }
                    }
                }
                tqRes.close();
//                log.info("{}, OutDegree process: {}/{}, size={}", connection.getRepository().toString(), i, subjects.size(), cnt);
            } catch (Exception ignored) {}

        }

        writer.write();

        return cnt;
    }

    private Set<String> sampleSubject(boolean weighted, int size) {
        Set<String> subjects = new HashSet<>();

        TupleQuery tq = null;
        TupleQueryResult tqRes = null;
        BindingSet bindingSet = null;
        Value v = null;
        while (subjects.size() < size) {

            if (weighted) {
                tq = connection.prepareTupleQuery("select ?s where { ?s ?p ?o } order by rand() limit 10000");
            } else {
                tq = connection.prepareTupleQuery("select distinct ?s where { ?s ?p ?o } order by rand() limit 10000");
            }

            try {
                tqRes = tq.evaluate();
                while (tqRes.hasNext() && subjects.size() < size) {
                    bindingSet = tqRes.next();
                    v = bindingSet.getValue("s");
                    if (v.isIRI()) {
                        subjects.add(v.stringValue());
                    }
                }

                tqRes.close();
            } catch (Exception ignored) {}

//            log.info("sample subject: {}/{}", subjects.size(), size);
        }

        return subjects;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public double getSize() {
        return size;
    }

    public void setSize(double size) {
        this.size = size;
    }

    public double getRatio() {
        return ratio;
    }

    public void setRatio(double ratio) {
        this.ratio = ratio;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
