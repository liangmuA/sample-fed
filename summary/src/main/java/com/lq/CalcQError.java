package com.lq;

import com.alibaba.fastjson.JSONArray;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CalcQError {

    public static void main(String[] args) throws IOException {
//        List<String> endpointNames = Arrays.asList("chebi", "dbpedia", "drugbank", "geonames", "jamendo", "kegg", "mdb", "nty", "swdf");
//        String[] ratios = {"0.01", "0.005", "0.001"};
//        String[] methods = {"weighted", "hybrid", "unweighted"};
//
//        for (String endpointName : endpointNames) {
//            List<List<List<Double>>> methodList = new ArrayList<>();
//            for (String method : methods) {
//                List<List<Double>> ratioList = new ArrayList<>();
//                for (String ratio : ratios) {
//                    List<Double> qError = new ArrayList<>();
//                    String line;
//                    String originFile = "summary/q-error1/" + endpointName + "/" + ratio + "/" + method + ".csv";
//                    Scanner in = new Scanner(new FileInputStream(new File(originFile)));
//                    while (in.hasNextLine()) {
//                        line = in.nextLine();
//                        String[] tmp = line.split(",");
//                        for (int j = 0; j < tmp.length; j += 2) {
//                            double a = Double.parseDouble(tmp[j]);
//                            double b = Double.parseDouble(tmp[j+1]);
//
//                            if (b <= 10) {
//                                continue;
//                            }
//
//                            if (a != 0 && b != 0) {
//                                qError.add(Math.log10(Math.max(a/b, b/a)));
//                            }
//                        }
//                    }
//                    ratioList.add(qError);
//                }
//                methodList.add(ratioList);
//            }
//            String recordFile = "summary/q-error1/" + endpointName + "/record.json";
//            OutputStream out = new FileOutputStream(new File(recordFile));
//            out.write(methodList.toString().getBytes(StandardCharsets.UTF_8));
//            out.flush();
//            out.close();
//        }
        List<String> endpointNames = Arrays.asList("chebi", "dbpedia", "drugbank", "geonames", "jamendo", "kegg", "mdb", "nty", "swdf");
        String[] qErrorNames = {"q-error1", "q-error5", "q-error3"};

        for (String endpointName : endpointNames) {
            JSONArray array = new JSONArray();
            for (String qErrorName : qErrorNames) {
                String originFile = "summary/" + qErrorName + "/" + endpointName + "/record.json";
                array.add(JSONArray.parseArray(FileUtils.readFileToString(new File(originFile), StandardCharsets.UTF_8)));
            }
            File file = new File("summary/q-error/" + endpointName + ".json");
            FileUtils.write(file, array.toJSONString(), StandardCharsets.UTF_8);
        }
    }

}
