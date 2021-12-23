package com.lq;

import com.alibaba.fastjson.JSONArray;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GeneraSummary {

    public static double[][] tg = {
            {0.82, 0.94, 0.96, 0.94, 1, 1, 1, 0.96, 1.14},
            {10.76, 2.78, 2.27, 0.97, 2.79, 16.1, 1.35, 4.09, 2.2},
            {5.84, 1.88, 1.66, 0.96, 1.95, 10.25, 1.18, 2.58, 1.61},
            {0.35, 0.47, 0.46, 0.48, 0.49, 0.55, 0.5, 0.53, 0.58},
            {5.44, 1.4, 1.21, 0.48, 1.43, 9.65, 0.68, 2.17, 1.07},
            {2.88, 0.95, 0.86, 0.49, 0.98, 5.76, 0.59, 1.32, 0.85},
            {0.07, 0.09, 0.09, 0.55, 0.09, 0.07, 0.1, 0.09, 0.1},
            {1.15, 0.27, 0.24, 0.57, 0.29, 2.21, 0.13, 0.43, 0.22},
            {0.7, 0.18, 0.17, 0.56, 0.2, 1.04, 0.12, 0.3, 0.15}
    };

    /*
            * {1, 1, 1, 1, 1, 1, 1, 1, 1},
            {1, 1, 1, 1, 1, 1, 1, 1, 1},
            {1, 1, 1, 1, 1, 1, 1, 1, 1},
            {0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5},
            {0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5},
            {0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5},
            {0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1},
            {0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1},
            {0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1}
    * */

    public static JSONArray originInfo;

    static {
        try {
            originInfo = JSONArray.parseArray(FileUtils.readFileToString(new File("sampler/sample/p.json"), StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        String host = "106.55.187.231";

        List<String> endpointUrl = Arrays.asList(
                "http://" + host + ":8890/sparql",    // chebi
                "http://" + host + ":8891/sparql",    // dbpedia
                "http://" + host + ":8892/sparql",    // drugbank
                "http://" + host + ":8893/sparql",    // geonames
                "http://" + host + ":8894/sparql",    // jamendo
                "http://" + host + ":8895/sparql",    // kegg
                "http://" + host + ":8896/sparql",    // mdb
                "http://" + host + ":8897/sparql",    // nty
                "http://" + host + ":8898/sparql"    // swdf
        );

        List<String> endpointName = Arrays.asList(
                "chebi",
                "dbpedia",
                "drugbank",
                "geonames",
                "jamendo",
                "kegg",
                "mdb",
                "nty",
                "swdf"
        );

        int branchLimit = 4;

        double[] sizes = {0.01, 0.005, 0.001};
        String[] methods = {"unweighted", "weighted", "hybrid"};

        for (int i = 0; i < sizes.length; i++) {
            for (int j = 0; j < methods.length; j++) {

                File file = new File("summary/summaries5/" + sizes[i] + "/" + methods[j] + "/record.txt");

                if (!file.exists()) {
                    File folder = new File(file.getParent());
                    if (!folder.exists()) {
                        folder.mkdirs();
                    }
                    file.createNewFile();
                }

                OutputStream out = new FileOutputStream(file);

                for (int k = 0; k < 10; k++) {
                    long start = System.currentTimeMillis();

                    List<String> rdfPaths = new ArrayList<>();
                    for (String s : endpointName) {
                        rdfPaths.add("sampler/sample/" + s + "/" + sizes[i] + "/" + methods[j] + "/" + methods[j] + k + ".nt");
                    }

                    Summary summary = new Summary("summary/summaries5/" + sizes[i] + "/" + methods[j] + "/" + methods[j] + k + ".n3");

                    summary.generateSummaries(endpointUrl, rdfPaths, branchLimit, tg[i * 3 + j]);

                    long end = System.currentTimeMillis();

                    out.write(("" + (end - start) / 1000).getBytes(StandardCharsets.UTF_8));
                    out.write('\n');
                    out.flush();
                }

                out.close();
            }
        }
    }

}
