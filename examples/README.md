# TODO:
- [X] Canonicalization Layer
    - [X] Trigger layer: package_list.py
    - [X] Crawling Layer: download_latest.py
    - [X] Tabularize: json2dataframe.py
    - [X] Rename file and Group into Same Layer
- [X] Sub-graph Extraction Layer
    - [X] Extract Nodes
    - [X] Extract Links
    - [X] Define relationship between node types and link types
    - [X] ID validate
- [ ] Graph Merging Layer
    - [X] Build messy->canon mapping ETL layer
        - [X] Generate mapping table for messy node source.
        - [X] Do messy->canon convertion on the messy nodes
            - [X] Convert Node Table
            - [X] Convert Link Table
    - [X] Group Nodes After Canon Clean Up
    - [X] Entity Resolution on the all messy node.
        - [X] Extract non-canon nodes. 
        - [X] Do entity resolution on non-canon nodes
        - [X] Do non-canon->cluster_id convertion 
            - [X] Convert Node Table
            - [X] Convert Link Table
    - [ ] ID validate on grouped links.
    - [ ] Enable no canon entity resolution flow.
- [X] Add Link Node Id convertion to group merging layer
- [ ] Make local file operation general for FileSystem class
