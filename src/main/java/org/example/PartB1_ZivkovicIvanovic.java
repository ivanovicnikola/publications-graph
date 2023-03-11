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
                    MATCH (c:Conference)
                    OPTIONAL MATCH (c{conference:conference})<-[:PART_OF]-(p:Proceeding)<-[:PUBLISHED_IN]-(:Inproceeding)-[:AUTHORED_BY]-(a:Author{author:author})
                    RETURN c.conference AS conference, COLLECT (DISTINCT a.author) AS community
                    ORDER BY SIZE(community) DESC
                    """);
            while (result.hasNext())
            {
                Record record = result.next();
                System.out.println("conference: " +  record.get("conference").asString() + ", community: " + record.get("community").asList());
            }
        }
    }

    public void executeQuery3() {
        System.out.println("Executing query 3...");
        try (Session session = driver.session()) {

            Result result = session.run("""
                    MATCH (article:Article)-[:PUBLISHED_IN]->(journal:Journal)
                    WITH journal, article.year AS year, COLLECT(article) AS articleList
                    MATCH (article:Article)-[:PUBLISHED_IN]->(journal)
                    WHERE article.year = year - 1
                    WITH journal, year, articleList, COLLECT(article) AS previousArticleList
                    MATCH (p:Article|Inproceeding)-[c:CITES]->(a:Article)-[:PUBLISHED_IN]->(journal)
                    WHERE p.year = year + 1 AND (a IN articleList + previousArticleList)
                    WITH journal, p.year as Year, COUNT(c) AS citations, size(articleList + previousArticleList) AS numberOfArticles
                    RETURN journal.journal AS Journal, Year, citations*1.0/numberOfArticles AS Impact_factor, citations, numberOfArticles
                    """);
            while (result.hasNext()) {
                Record record = result.next();
                System.out.println("Journal: " + record.get("Journal").asString() + ", Year: " + record.get("Year").asInt() + ", Impact factor: " + record.get("Impact_factor").asDouble());
            }
        }
    }


    public void executeQuery4() {
        System.out.println("Executing query 4...");
        try (Session session = driver.session())
        {
            Result result = session.run("""
                    MATCH ()<-[c:CITES]-(p)-[:AUTHORED_BY]->(a:Author)
                    WITH a.author AS author, p.title AS paper, COUNT(c) AS citations
                    ORDER BY author, citations DESC
                    WITH author, COLLECT(citations) AS cite_list
                    WITH author, cite_list, [x IN range (0, SIZE(cite_list)-1) WHERE x < cite_list[x]] AS indexes
                    RETURN author, SIZE(indexes) AS h_index
                    """);
            while (result.hasNext())
            {
                Record record = result.next();
                System.out.println("author: " +  record.get("author").asString() + ", h-index: " + record.get("h_index").asInt());
            }
        }
    }

    public static void main(String... args) {
        try(var loader = new PartB1_ZivkovicIvanovic("bolt://localhost:7687", "", "")) {
            loader.executeQuery1();
            loader.executeQuery2();
            loader.executeQuery3();
            loader.executeQuery4();
        }
    }
}
