package org.example;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Query;

import static org.neo4j.driver.Values.parameters;

public class PartA2_ZivkovicIvanovic implements AutoCloseable {
    private final Driver driver;

    public PartA2_ZivkovicIvanovic(String uri, String user, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public void loadAuthors() {
        try (var session = driver.session()) {
            session.run("""
                    LOAD CSV WITH HEADERS FROM 'file:///output_author.csv' AS line FIELDTERMINATOR ';'
                    CALL {
                        WITH line
                        CREATE (:Author {ID: line.ID, author: line.author})
                    }
                    IN TRANSACTIONS
                    """);
        }
    }

    public static void main(String... args) {
        try (var loader = new PartA2_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.loadAuthors();
        }
    }
}
