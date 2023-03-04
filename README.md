# Publications Graph
 Propery graphs assignment for the Semantic Data Management (SDM) course at Universitat Politècnica de Catalunya
## Requirements to run
 Java Version: 17
 Neo4j Version: 5.5.0
 Apoc Version: 5.5.0
## Steps to run the solution
1. Download DBLP data (https://dblp.uni-trier.de/)
2. Use the following tool to convert DBLP data from XML to CSV: https://github.com/ThomHurks/dblp-to-csv

```
python XMLToCSV.py  dblp.xml dblp.dtd output.csv
```
3. Place the generated csv files in the neo4j import directory
4.  Open output_article.csv and output_inproceedings.csv files in an editor and replace all \\"" with an empty string
5.  Execute PartA2_ZivkovicIvanovic.java to create the graph
