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

    public void connectAuthorsToJournals() {
        System.out.println("Connecting authors to journals...");
        try (var session = driver.session()) {
            session.run("""
                    MATCH p=(a:Author)<-[:AUTHORED_BY]-(:Article)-[:PUBLISHED_IN]->(j:Journal)
                    MERGE (a)-[:WRITES_FOR]->(j)
                    """);
        }
    }

    public void connectAuthorsToConferences() {
        System.out.println("Connecting authors to conferences...");
        try (var session = driver.session()) {
            session.run("""
                    MATCH p=(a:Author)<-[:AUTHORED_BY]-(:Inproceeding)-[:PUBLISHED_IN]->(:Proceeding)-[:PART_OF]->(c:Conference)
                    MERGE (a)-[:WRITES_FOR]->(c)
                    """);
        }
    }

    public void createWritesForSubgraph() {
        System.out.println("Creating subgraph...");
        try (var session = driver.session()) {
            session.run("""
                    MATCH (a:Author)-[:WRITES_FOR]->(p:Journal|Conference)
                    RETURN gds.alpha.graph.project('writesFor', a, p)
                    """);
        }
    }

    public void callNodeSimilarity() {
        System.out.println("Finding similar authors...");
        try (var session = driver.session()) {
            Result result = session.run("""
                    CALL gds.nodeSimilarity.stream('writesFor')
                    YIELD node1, node2, similarity
                    RETURN gds.util.asNode(node1).author AS Author1, gds.util.asNode(node2).author AS Author2, similarity
                    ORDER BY similarity DESCENDING, Author1, Author2
                    """);
            while (result.hasNext())
            {
                Record record = result.next();
                System.out.println("Author1: " +  record.get("Author1").asString() + ", Author2: " + record.get("Author2").asString() + ", similarity: " + record.get("similarity").asDouble());
            }
        }
    }

    public void dropWritesForSubgraph() {
        System.out.println("Dropping subgraph...");
        try (var session = driver.session()) {
            session.run("""
                    CALL gds.graph.drop('writesFor')
                    """);
        }
    }

    public void deleteWritesForRelationships() {
        System.out.println("Dropping writes for relationships...");
        try (var session = driver.session()) {
            session.run("""
                    MATCH ()-[r:WRITES_FOR]->() DELETE r
                    """);
        }
    }

    public static void main(String... args) {
        //Run only if C is not ran beforehand
        /*try(var loader = new PartC_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.findJournalCommunity();
        }*/
        try(var loader = new PartD_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            //betweenness
            loader.createSubgraph();
            loader.callBetweenness();
            loader.dropSubgraph();
            //node similarity
            loader.connectAuthorsToJournals();
            loader.connectAuthorsToConferences();
            loader.createWritesForSubgraph();
            loader.callNodeSimilarity();
            loader.dropWritesForSubgraph();
            loader.deleteWritesForRelationships();
        }
    }
}
