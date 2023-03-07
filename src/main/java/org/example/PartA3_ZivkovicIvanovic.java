package org.example;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

public class PartA3_ZivkovicIvanovic implements AutoCloseable{

    private final Driver driver;
    private static final int SCHOOL_LIMIT = 100;

    public PartA3_ZivkovicIvanovic(String uri, String user, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public void createReviews() {
        System.out.println("Creating reviews...");
        try (var session = driver.session()) {
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        MATCH (p)-[r:REVIEWED_BY]->(a)
                        DELETE r
                        CREATE (p)-[:HAS_REVIEW]->(:Review {content: 'Review content', accepted: apoc.coll.randomItem(range(0, 1))})-[:WRITTEN_BY]->(a)
                        """);
            });
        }
    }

    public void addAcceptanceStatus() {
        System.out.println("Adding acceptance status...");
        try (var session = driver.session()) {
            session.executeWriteWithoutResult(tx -> {
                tx.run("""
                        MATCH (p:Article|Inproceeding)
                        WITH p, CASE
                            WHEN COUNT {(p)-[:HAS_REVIEW]->(:Review {accepted: 1})} > 1 THEN 1
                            ELSE 0
                        END AS accepted
                        SET p.accepted = accepted
                        """);
            });
        }
    }

    public void loadSchools() {
        System.out.println("Loading schools...");
        try (var session = driver.session()) {
            session.run(String.format("""
                    LOAD CSV FROM 'file:///output_school.csv' AS line FIELDTERMINATOR ';'
                    WITH line SKIP 1 LIMIT %d
                    CALL {
                        WITH line
                        CREATE (:School {school: line[1]})
                    }
                    IN TRANSACTIONS
                    """, SCHOOL_LIMIT));
        }
    }

    public void loadCompanies() {
        System.out.println("Loading companies...");
        try (var session = driver.session()) {
            session.run(String.format("""
                    LOAD CSV FROM 'file:///output_publisher.csv' AS line FIELDTERMINATOR ';'
                    WITH line SKIP 1 LIMIT %d
                    CALL {
                        WITH line
                        CREATE (:Company {company: line[1]})
                    }
                    IN TRANSACTIONS
                    """, SCHOOL_LIMIT));
        }
    }

    public void generateAffiliations() {
        System.out.println("Generating random affiliations...");
        try (var session = driver.session()) {
            session.run("""
                    MATCH (org:School|Company)
                    WITH collect(org) as organizations
                    MATCH (a:Author)
                    WITH a, apoc.coll.randomItem(organizations) as org
                    CREATE (a)-[:HAS_AFFILIATION]->(org)
                    """);
        }
    }

    public static void main(String... args) {
        try(var loader = new PartA3_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.createReviews();
            loader.addAcceptanceStatus();
            loader.loadSchools();
            loader.loadCompanies();
            loader.generateAffiliations();
        }
    }
}
