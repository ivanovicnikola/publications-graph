package org.example;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

public class PartA2_ZivkovicIvanovic implements AutoCloseable {
    private final Driver driver;

    private static final int ARTICLE_LIMIT = 1000;
    private static final int PROCEEDING_LIMIT = ARTICLE_LIMIT/10;
    private static final int MIN_CITATIONS = 0;
    private static final int MAX_CITATIONS = 10;

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
                        CREATE CONSTRAINT articleKeyConstraint IF NOT EXISTS
                        FOR (article:Article)
                        REQUIRE article.key IS UNIQUE
                        """);
            });
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        CREATE CONSTRAINT inproceedingKeyConstraint IF NOT EXISTS
                        FOR (inproceeding:Inproceeding)
                        REQUIRE inproceeding.key IS UNIQUE
                        """);
            });
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        CREATE CONSTRAINT proceedingKeyConstraint IF NOT EXISTS
                        FOR (proceeding:Proceeding)
                        REQUIRE proceeding.key IS UNIQUE
                        """);
            });
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        CREATE CONSTRAINT authorConstraint IF NOT EXISTS
                        FOR (author:Author)
                        REQUIRE author.author IS UNIQUE
                        """);
            });
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        CREATE CONSTRAINT journalConstraint IF NOT EXISTS
                        FOR (journal:Journal)
                        REQUIRE journal.journal IS UNIQUE
                        """);
            });
        }
    }

    public void loadArticles() {
        System.out.println("Loading articles...");
        try (var session = driver.session()) {
            session.run(String.format("""
                    LOAD CSV WITH HEADERS FROM 'file:///output_article.csv' AS line FIELDTERMINATOR ';'
                    WITH line LIMIT %d
                    CALL {
                        WITH line
                        MERGE (j:Journal {journal: line.journal})
                        CREATE (a:Article {mdate: date(line.mdate), key: line.key, publtype: line.publtype,
                            title: line.title, month: line.month, year: toInteger(line.year)})
                        CREATE (a)-[:PUBLISHED_IN {volume: line.volume}]->(j)
                        WITH line, a
                        UNWIND split(line.author, '|') AS authors
                        MERGE (author:Author {author: authors})
                        CREATE (a)-[:AUTHORED_BY]->(author)
                        WITH author, a, line
                        WHERE author.author = split(line.author, '|')[0]
                        CREATE (a)-[:CORRESPONDING_AUTHOR]->(author)
                    }
                    IN TRANSACTIONS
                    """, ARTICLE_LIMIT));
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
                        CREATE (:Proceeding {key: line.key, mdate: date(line.mdate), 
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
                        CREATE (i:Inproceeding {key: line.key, mdate: date(line.mdate), 
                        title: line.title, year: toInteger(line.year), booktitle: line.booktitle}) -[:PUBLISHED_IN]->(p)
                        WITH line, i
                        UNWIND split(line.author, '|') AS authors
                        MERGE (author:Author {author: authors})
                        CREATE (i)-[:AUTHORED_BY]->(author)
                        WITH author, i, line
                        WHERE author.author = split(line.author, '|')[0]
                        CREATE (i)-[:CORRESPONDING_AUTHOR]->(author)
                    }
                    IN TRANSACTIONS
                    """);
        }
    }

    public void generateReviews() {
        System.out.println("Generating random reviews...");
        try (var session = driver.session()) {
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        MATCH (p:Article|Inproceeding)
                        WITH p
                        MATCH (a:Author) WHERE NOT (p)-[:AUTHORED_BY]->(a)
                        WITH p, apoc.coll.randomItems(COLLECT(a), 3) as authors
                        FOREACH (author IN authors | CREATE (p)-[:REVIEWED_BY]->(author))
                        """);
            });
        }
    }

    public void generateCitations() {
        System.out.println("Generating random citations...");
        try (var session = driver.session()) {
            session.executeWriteWithoutResult(tx -> {
                tx.run(String.format("""
                        MATCH (p1:Article|Inproceeding)
                        WITH p1
                        MATCH (p2:Article|Inproceeding) WHERE p1.key <> p2.key
                        WITH p1, apoc.coll.randomItems(COLLECT(p2), apoc.coll.randomItem(range(%d, %d))) as papers
                        FOREACH (paper IN papers | CREATE (p1)-[:CITES]->(paper))
                        """, MIN_CITATIONS, MAX_CITATIONS));
            });
        }
    }

    public static void main(String... args) {
        try (var loader = new PartA2_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.createConstraints();
            loader.loadArticles();
            loader.loadProceedings();
            loader.loadInproceedings();
            loader.generateReviews();
            loader.generateCitations();
        }
    }
}
