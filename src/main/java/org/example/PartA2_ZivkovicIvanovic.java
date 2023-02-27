package org.example;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Query;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
        System.out.println("Loading authors...");
        try (var session = driver.session()) {
            session.run("""
                    LOAD CSV WITH HEADERS FROM 'file:///output_author.csv' AS line FIELDTERMINATOR ';'
                    CALL {
                        WITH line
                        CREATE (:Author {ID: toInteger(line.ID), author: line.author})
                    }
                    IN TRANSACTIONS
                    """);
        }
        System.out.println("Authors loaded.");
    }

    public void loadArticles() {
        System.out.println("Loading articles...");
        try (var session = driver.session()) {
            session.run("""
                    LOAD CSV WITH HEADERS FROM 'file:///output_article.csv' AS line FIELDTERMINATOR ';'
                    CALL {
                        WITH line
                        CREATE (:Article {ID: toInteger(line.ID), mdate: date(line.mdate), key: line.key, publtype: line.publtype,
                            title: line.title, volume: line.volume, month: line.month, year: toInteger(line.year)})
                    }
                    IN TRANSACTIONS
                    """);
        }
        System.out.println("Articles loaded.");
    }

    public static void main(String... args) {
        try (var loader = new PartA2_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.loadAuthors();
            loader.loadArticles();
        }
    }
}
