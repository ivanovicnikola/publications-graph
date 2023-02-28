package org.example;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

public class PartA2_ZivkovicIvanovic implements AutoCloseable {
    private final Driver driver;

    private static final int ENTITY_LIMIT = 10000;
    private static final int PROCEEDING_LIMIT = 1000;

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
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        CREATE CONSTRAINT inproceedingIdConstraint IF NOT EXISTS
                        FOR (inproceeding:Inproceeding)
                        REQUIRE inproceeding.ID IS UNIQUE
                        """);
            });
        }
    }

    public void createIndexes() {
        System.out.println("Creating indexes...");
        try (var session = driver.session()) {
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        CREATE INDEX proceedingKeyIndex IF NOT EXISTS FOR (p:Proceeding) ON (p.key)
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
        System.out.println("Relating papers with authors...");
        try (var session = driver.session()) {
            session.run("""
                    LOAD CSV FROM 'file:///output_author_authored_by.csv' AS line FIELDTERMINATOR ';'
                    CALL {
                        WITH line
                        MATCH (paper:Article|Inproceeding {ID: toInteger(line[0])}), (author:Author {ID: toInteger(line[1])})
                        CREATE (paper)-[:AUTHORED_BY]->(author)
                    }
                    IN TRANSACTIONS
                    """);
        }
    }

    public void loadProceedings() {
        System.out.println("Loading proceedings...");
        try (var session = driver.session()) {
            session.run(String.format("""
                    LOAD CSV WITH HEADERS FROM 'file:///output_proceedings.csv' AS line FIELDTERMINATOR ';'
                    WITH line LIMIT %d
                    CALL {
                        WITH line
                        CREATE (:Proceeding {ID: toInteger(line.id), key: line.key, mdate: date(line.mdate), 
                        title: line.title, volume: line.volume, year: toInteger(line.year), booktitle: line.booktitle})
                    }
                    IN TRANSACTIONS
                    """, PROCEEDING_LIMIT));
        }
    }

    public void loadInproceedings() {
        System.out.println("Loading inproceedings...");
        try (var session = driver.session()) {
            session.run("""
                    LOAD CSV WITH HEADERS FROM 'file:///output_inproceedings.csv' AS line FIELDTERMINATOR ';'
                    CALL {
                        WITH line
                        MATCH (p:Proceeding {key: line.crossref})
                        CREATE (i:Inproceeding {ID: toInteger(line.id), key: line.key, mdate: date(line.mdate), 
                        title: line.title, year: toInteger(line.year), booktitle: line.booktitle}) -[:PUBLISHED_IN]->(p)
                    }
                    IN TRANSACTIONS
                    """);
        }
    }

    public static void main(String... args) {
        try (var loader = new PartA2_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.createConstraints();
            loader.createIndexes();
            loader.loadAuthors();
            loader.loadArticles();
            loader.loadProceedings();
            loader.loadInproceedings();
            loader.loadAuthoredBy();
        }
    }
}
