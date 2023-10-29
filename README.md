# Intro
Experiment with new ETL architecture: using Dropbox as storage, Duckdb + Python as Operator, Github Action as scheduler

# Steps:

1. Setup connection to Dropbox
2. Build `ETL` object and `Storage` objects
    - `ETL` for defining etl logic. 
        - execute method: run etl
        - input_ids / output_ids: a list of object ids to be recognized by `Storage`
    - `Storage` for upload and download python object from remote storage. 
3. Build `Scheduler` class takes etl as input and connect etl instance, input_ids, output_ids with `paradag`.
4. Using paradag to generate execution order and stetch the execution to Github Action.
5. Build a `Layer` class which can define layer-wise 
ETL function and a `Platform` class in which allow `Layers` to be stacked.  logic. 
    - `Layer`: 
        - input_ids
        - output_ids
        - operations: a list of `ETL` class with inputs_ids and output_ids mapped to those of `Layer` object. (Should be same subclass)
        - __init__: same args and kwargs setup as the `ETL` class in operations
    - `Platform` Take multiple layers as input and connect the underlying `ETL` class to Scheduler
# Future Plan: 

1) Extend to various web sources
2) Build layered ETL pipeline
3) Build Graph from PyPi

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

