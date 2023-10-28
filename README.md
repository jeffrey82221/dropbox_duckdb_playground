# Intro
Experiment with new ETL architecture: using Dropbox as storage, Duckdb + Python as Operator, Github Action as scheduler

# Steps:

1. Setup connection to Google Drive following: https://pythonhosted.org/PyDrive/quickstart.html

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
    - info.package_url
    - info.project_url
    - info.home_page
    - info.project_urls (Dict)

## Layer Design

- Zero Layer: save raw json into dropbox
- Blonze Layer: convert json to tabular data
    - one table for all the simple fields
    - multiple tables each for one complex field
- Silver 1: extract node and link tables
- Silver 2: group related nodes using AI and logic 
    - Replace Node ID(s) in Node and Link Table of Silver 1 after AI clustering
    - Group Related Nodes and Related Links
- RedisGraph Layer:
    - ingest node and link to redisgraph

