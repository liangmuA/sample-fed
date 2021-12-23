package com.lq;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class SampleWriter {

    private static final Logger log = LoggerFactory.getLogger(SampleWriter.class);

    private String outPath;
    private ModelBuilder builder;

    public SampleWriter(String outPath) {
        this.outPath = outPath;
        builder = new ModelBuilder();

        try {
            File file = new File(outPath);

            if (!file.exists()) {
                File folder = new File(file.getParent());

                if (!folder.exists()) {
                    folder.mkdirs();
                }

                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void add(String s, IRI p, Object o) {
        builder.add(s, p, o);
    }

    public void write() {
        Model model = builder.build();

        FileOutputStream out = null;
        try {
            out = new FileOutputStream(outPath);
            RDFWriter writer = Rio.createWriter(RDFFormat.N3, out);
            writer.startRDF();
            for (Statement st: model) {
                writer.handleStatement(st);
            }
            writer.endRDF();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.info("sample generate success, file stored: {}", outPath);
        }
    }

}
