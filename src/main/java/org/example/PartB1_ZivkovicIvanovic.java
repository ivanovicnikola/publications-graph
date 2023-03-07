package org.example;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class PartB1_ZivkovicIvanovic implements AutoCloseable {

    private Driver driver;

    public PartB1_ZivkovicIvanovic(String uri, String user, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public void executeQuery1() {
        System.out.println("Executing query 1...");
        try (Session session = driver.session())
        {
            Result result = session.run("""
                    MATCH (p:Inproceeding|Article)-[c:CITES]->(i:Inproceeding)-[:PUBLISHED_IN]->(pr:Proceeding)-[:PART_OF]->(conf:Conference)
                    WITH i, conf, COUNT(c) AS cnt
                    ORDER BY COUNT(c) DESC
                    WITH conf.conference AS conferences, collect(i.title) AS inproceeding, collect(cnt) AS counts
                    RETURN conferences, inproceeding[0..3] AS inproceedings, counts[0..3] as number
                    ORDER BY conferences
                    """);
            while (result.hasNext())
            {
                Record record = result.next();
                System.out.println("Conference: " +  record.get("conferences").asString() + "\nInproceedings: " + record.get("inproceedings").asList() + "\nNumber of citations: " + record.get("number").asList() + "\n__________________________________________________________");
            }
        }
    }

    public void executeQuery2() {
        System.out.println("Executing query 2...");
        try (Session session = driver.session())
        {
            Result result = session.run("""
                    MATCH (c:Conference)<-[:PART_OF]-(p:Proceeding)<-[:PUBLISHED_IN]-(:Inproceeding)-[:AUTHORED_BY]-(a:Author)
                    WITH c.conference AS conference, a.author AS author, COUNT(DISTINCT p) AS cnt
                    WHERE cnt > 3
                    RETURN conference, COLLECT(author) AS community
                    """);
            while (result.hasNext())
            {
                Record record = result.next();
                System.out.println("conference: " +  record.get("conference").asString() + ", community: " + record.get("community").asList());
            }
        }
    }

    public static void main(String... args) {
        try(var loader = new PartB1_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.executeQuery1();
            loader.executeQuery2();
        }
    }
}
