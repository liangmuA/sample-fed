package org.aksw.simba.start;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fluidops.fedx.DefaultEndpointListProvider;
import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.sail.FedXSailRepository;

public class Test {

    public static OutputStream out;

    static Logger log = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) throws Exception {
        String host = "119.91.217.63";
        List<String> endpoints= Arrays.asList(
                "http://" + host + ":8890/sparql",	// chebi
                "http://" + host + ":8891/sparql",	// dbpedia
                "http://" + host + ":8892/sparql",	// drugbank
                "http://" + host + ":8893/sparql",	// geonames
                "http://" + host + ":8894/sparql",	// jamendo
                "http://" + host + ":8895/sparql",	// kegg
                "http://" + host + ":8896/sparql",	// mdb
                "http://" + host + ":8897/sparql",	// nty
                "http://" + host + ":8898/sparql"	// swdf
        );

        String size = "0.001";
        String method = "hybrid";

//        int i = 5;
        for (int i = 5; i < 10; i++) {
            String resPath = "D:\\federated\\costfed\\results3\\" + size + "\\" + method + "\\" + method + i + ".csv";
            File file = new File(resPath);
            if (!file.exists()) {
                File folder = new File(file.getParent());
                if (!folder.exists()) {
                    folder.mkdirs();
                }
                file.createNewFile();
            }
            out = new FileOutputStream(resPath, true);
            Scanner scanner = new Scanner(new FileInputStream("D:\\federated\\costfed\\costfed.props"));
            OutputStream out1 = new FileOutputStream("D:\\federated\\costfed\\costfed_copy.props");
            for (int j = 0; j < 35; j++)  {
                String line = scanner.nextLine();
                if (line.startsWith("quetzal.fedSummaries")) {
                    line = "quetzal.fedSummaries=" + "D:\\\\federated\\\\summary\\\\summaries3\\\\" + size + "\\\\" + method + "\\\\" + method + i + ".n3";
                }
                out1.write(line.getBytes(StandardCharsets.UTF_8));
                out1.write('\n');
                out1.flush();
            }
            out1.close();
            FedXSailRepository rep = FedXFactory.initializeFederation("D:\\federated\\costfed\\costfed_copy.props",
                    new DefaultEndpointListProvider(endpoints));
//            try {
                RepositoryConnection conn = rep.getConnection();
                List<String> queries = Arrays.asList("CD1", "CD2", "CD3", "CD4", "CD5", "CD6", "CD7", "LS1", "LS2", "LS3", "LS4", "LS5", "LS6", "LS7", "LD1", "LD2", "LD3", "LD4", "LD5", "LD6", "LD7", "LD8", "LD9", "LD10", "LD11");
                for (int j = 0; j < 25; j++){
                    String query = queries.get(j);
                    out.write((j + "," + query).getBytes(StandardCharsets.UTF_8));
                    String qp = "D:\\federated\\queries\\" + query + ".rq";
                    InputStream in = new FileInputStream(qp);
                    byte[] bytes = new byte[1024];
                    int len = 0;
                    StringBuilder sb = new StringBuilder();
                    while ((len = in.read(bytes)) != -1) {
                        sb.append(new String(bytes, 0, len));
                    }

                    TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, sb.toString());

                    long result = 0;
                    double time = 0.0;

                    long startTime = System.currentTimeMillis();
                    TupleQueryResult res = tq.evaluate();
                    while (res.hasNext()) {
                        BindingSet row = res.next();
                        result++;
                    }
                    long endTime = System.currentTimeMillis();

                    time += endTime - startTime;
                    res.close();

                    out.write(("," + result).getBytes(StandardCharsets.UTF_8));
                    out.write(("," + time/1000).getBytes(StandardCharsets.UTF_8));
                    out.write('\n');
                    out.flush();

                }

                out.close();

                conn.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            finally {
                rep.shutDown();
//            }

        }
    }
}
