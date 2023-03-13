package org.example;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class PartD_ZivkovicIvanovic implements AutoCloseable {

    private Driver driver;

    public PartD_ZivkovicIvanovic(String uri, String user, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public void createSubgraph() {
        System.out.println("Creating subgraph...");
        try (var session = driver.session()) {
            session.run("""
                    MATCH p=(j1:Journal)<-[:PUBLISHED_IN]-(a1:Article)-[:CITES]->(a2:Article)-[:PUBLISHED_IN]->(j2:Journal)
                    WHERE j1 <> j2 AND NOT EXISTS {
                        MATCH (j1)-[:BELONGS_TO]->(:Community)<-[:BELONGS_TO]-(j2)
                    }
                    RETURN gds.alpha.graph.project('subgraph', a1, a2)
                    """);
        }
    }

    public void callBetweenness() {
        System.out.println("Determining journal interdisciplinarity ...");
        try (var session = driver.session()) {
            Result result = session.run("""
                    CALL gds.betweenness.stream(
                        'subgraph'
                    )
                    YIELD nodeId, score
                    WITH gds.util.asNode(nodeId) AS article, score
                    MATCH (article)-[:PUBLISHED_IN]->(j:Journal)
                    RETURN j.journal AS journal, AVG(score) AS average
                    ORDER BY average DESC, journal ASC
                    """);
            while (result.hasNext())
            {
                Record record = result.next();
                System.out.println("journal: " +  record.get("journal").asString() + ", score: " + record.get("average").asDouble());
            }
        }
    }

    public void dropSubgraph() {
        System.out.println("Dropping subgraph...");
        try (var session = driver.session()) {
            session.run("""
                    CALL gds.graph.drop('subgraph')
                    """);
        }
    }

    public static void main(String... args) {
        try(var loader = new PartD_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.createSubgraph();
            loader.callBetweenness();
            loader.dropSubgraph();
        }
    }
}
