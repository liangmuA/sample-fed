package com.lq;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class Storage {

    private static final Logger log = LoggerFactory.getLogger(Storage.class);

    private Repository repository;
    private RepositoryConnection connection;

    public Storage(String rdfPath) {

        InputStream is = null;
        Model model = null;
        try {
            is = new FileInputStream(rdfPath);

            model = Rio.parse(is, "", RDFFormat.N3);
        } catch (IOException e) {
            e.printStackTrace();
        }

        repository = new SailRepository(new MemoryStore());
        connection = repository.getConnection();

        connection.add(model);

        log.info("加载样本数据集: {}", rdfPath);
    }

    public Repository getRepository() {
        return repository;
    }

    public RepositoryConnection getConnection() {
        return connection;
    }

}
