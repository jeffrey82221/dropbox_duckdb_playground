# Intro
Experiment with new ETL architecture: using Dropbox as storage, Duckdb + Python as Operator, Github Action as scheduler

# Steps:

1. [ ] Setup connection to Dropbox
2. [X] Build `ETL` object and `Storage` objects
    - `ETL` for defining etl logic. 
        - execute method: run etl
        - input_ids / output_ids: a list of object ids to be recognized by `Storage`
    - `Storage` for upload and download python object from remote storage. 
3. [X] Build `ETLGroup` class takes etl as input and connect etl instance, input_ids, output_ids with `paradag`.
4. [ ] Speed up Mapping Method
5. Build `SchedulerAdaptor` class to generate YAML for Github Action
and Airflow dag-factory (can build dag from yaml)
    - ref: https://github.com/ajbosco/dag-factory
    
# Future Plan: 

1) [X] Build layered ETL pipeline
2) [X] Build Graph from PyPi
3) [ ] Replace batch_framework.filesystem by ffspec package (It is a filesystem interface used by many framework. e.g., pandas, torch, xarray, dask). 
4) [ ] Enable Cache Mechanism (using copy in filesystem) to store inputs and outputs of previous run for later use. (turn on only if set)
5) [ ] Enable Automatic Temporary Data Cleaning 
6) [ ] Change current feedback mechanism to common cache mechaism (check examples)
7) [ ] Study of dropboxdrivefs as filesystem. 
8) Extend to various web sources
# Related Fields:

## Ref:
    - https://raw.githubusercontent.com/jeffrey82221/PyPiMailCentor/main/data/latest.schema

## Node from latest.schema:

- Package Node:
    - info.name
    - info.package_url
    - info.project_url
    - info.requires_python
    - info.version
    - info.keywords
    - releases -> num_releases
- Requirement Package Node:
    - info.requires_dist (Array)
- Author Person Node:
    - info.author
    - info.author_email
- Maintainer Person Node:
    - info.maintainer
    - info.maintainer_email
- License Node:
    - info.license
- Related URLs
    - info.docs_url
    - info.home_page
    - info.project_urls (Dict)

## Layer Design

- Blonze Layer: Call API and convert json to tabular data
    - one table for all the simple fields
    - multiple tables each for one complex field
- Silver 1: extract node and link tables
- Silver 2: group related nodes using AI and logic 
    - Replace Node ID(s) in Node and Link Table of Silver 1 after AI clustering
    - Group Related Nodes and Related Links
- RedisGraph Layer:
    - ingest node and link to redisgraph

