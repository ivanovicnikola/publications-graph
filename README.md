# Publications Graph
 Propery graphs assignment for the Semantic Data Management (SDM) course at Universitat Politècnica de Catalunya

## Steps to run the solution
1.  Download DBLP data (https://dblp.uni-trier.de/)
2. Use the following tool to convert DBLP data from XML to CSV: https://github.com/ThomHurks/dblp-to-csv

```
python XMLToCSV.py  dblp.xml dblp.dtd output.csv --relations author:authored_by journal:published_in publisher:published_by school:submitted_at editor:edited_by    cite:has_citation series:is_part_of
```
3. Place the generated csv files in the neo4j import directory
4.  Open output_article.csv in an editor and replace all \\"" with an empty string
5.  Execute PartA2_ZivkovicIvanovic.java to create the graph
