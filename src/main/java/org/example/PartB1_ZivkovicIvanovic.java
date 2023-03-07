package org.example;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

public class PartB1_ZivkovicIvanovic implements AutoCloseable {

    private Driver driver;

    public PartB1_ZivkovicIvanovic(String uri, String user, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public static void main(String... args) {
        try(var loader = new PartB1_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {

        }
    }
}
