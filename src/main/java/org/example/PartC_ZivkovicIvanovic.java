package org.example;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

public class PartC_ZivkovicIvanovic implements AutoCloseable{

    private final Driver driver;

    public PartC_ZivkovicIvanovic(String uri, String user, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));;
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public void findJournalCommunity() {
        System.out.println("Relating journals to communities...");
        try (var session = driver.session()) {
            session.run("""
                    MATCH (j:Journal)<-[:PUBLISHED_IN]-(a:Article)-[:HAS_KEYWORD]->(k:Keyword)<-[:DEFINED_BY]-(c:Community)
                    WITH j, c, COUNT(DISTINCT a) AS cnt
                    MATCH (j)<-[:PUBLISHED_IN]-(a:Article)
                    WITH j, c, cnt, COUNT(a) AS total
                    WHERE cnt/total >= 0.9
                    CREATE (j)-[:BELONGS_TO]->(c)
                    """);
        }
    }

    public void findConferenceCommunity() {
        System.out.println("Relating conferences to communities...");
        try (var session = driver.session()) {
            session.run("""
                    MATCH (conf:Conference)<-[:PART_OF]-(p:Proceeding)<-[:PUBLISHED_IN]-(i:Inproceeding)-[:HAS_KEYWORD]->(k:Keyword)<-[:DEFINED_BY]-(c:Community)
                    WITH conf, c, COUNT(DISTINCT i) AS cnt
                    MATCH (conf)<-[:PART_OF]-(p:Proceeding)<-[:PUBLISHED_IN]-(i:Inproceeding)
                    WITH conf, c, cnt, COUNT(i) AS total
                    WHERE cnt/total >= 0.9
                    CREATE (conf)-[:BELONGS_TO]->(c)
                    """);
        }
    }

    public static void main(String... args) {
        try(var loader = new PartC_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.findJournalCommunity();
            loader.findConferenceCommunity();
        }
    }
}
