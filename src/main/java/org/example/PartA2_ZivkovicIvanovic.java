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

    private static final int ENTITY_LIMIT = 10000;

    public PartA2_ZivkovicIvanovic(String uri, String user, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public void createConstraints() {
        System.out.println("Creating constraints...");
        try (var session = driver.session()) {
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        CREATE CONSTRAINT authorIdConstraint IF NOT EXISTS
                        FOR (author:Author)
                        REQUIRE author.ID IS UNIQUE
                        """);
            });
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        CREATE CONSTRAINT articleIdConstraint IF NOT EXISTS
                        FOR (article:Article)
                        REQUIRE article.ID IS UNIQUE
                        """);
            });
        }
    }

    public void loadAuthors() {
        System.out.println("Loading authors...");
        try (var session = driver.session()) {
            session.run(String.format("""
                    LOAD CSV FROM 'file:///output_author.csv' AS line FIELDTERMINATOR ';'
                    WITH line LIMIT %d
                    CALL {
                        WITH line
                        CREATE (:Author {ID: toInteger(line[0]), author: line[1]})
                    }
                    IN TRANSACTIONS
                    """, ENTITY_LIMIT));
        }
    }

    public void loadArticles() {
        System.out.println("Loading articles...");
        try (var session = driver.session()) {
            session.run(String.format("""
                    LOAD CSV WITH HEADERS FROM 'file:///output_article.csv' AS line FIELDTERMINATOR ';'
                    WITH line LIMIT %d
                    WHERE NOT line.volume IS NULL
                    CALL {
                        WITH line
                        MERGE (j:Journal {journal: line.journal})
                        MERGE (v:Volume {volume: line.volume})-[:PART_OF]->(j)
                        CREATE (a:Article {ID: toInteger(line.id), mdate: date(line.mdate), key: line.key, publtype: line.publtype,
                            title: line.title, month: line.month, year: toInteger(line.year)})
                        CREATE (a)-[:PUBLISHED_IN]->(v)
                    }
                    IN TRANSACTIONS
                    """, ENTITY_LIMIT));
        }
    }

    public void loadAuthoredBy() {
        System.out.println("Relating articles with authors...");
        try (var session = driver.session()) {
            session.run("""
                    LOAD CSV FROM 'file:///output_author_authored_by.csv' AS line FIELDTERMINATOR ';'
                    CALL {
                        WITH line
                        MATCH (article:Article {ID: toInteger(line[0])}), (author:Author {ID: toInteger(line[1])})
                        CREATE (article)-[:AUTHORED_BY]->(author)
                    }
                    IN TRANSACTIONS
                    """);
        }
    }

    public static void main(String... args) {
        try (var loader = new PartA2_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.createConstraints();
            loader.loadAuthors();
            loader.loadArticles();
            loader.loadAuthoredBy();
        }
    }
}
