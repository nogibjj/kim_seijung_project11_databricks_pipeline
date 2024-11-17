Check CI/CD Status:

# Mini-project #11
#### Repo Title: Data Pipeline with Databricks
#### Author: Seijung Kim (sk591)

## Overview
This project aims to package a simple Python program into a Rust-based Command Line Interface (CLI) that the user can install and use. This command line tool built with Rust allows manipulating an SQLite database. Using this tool you can load raw data from a csv and perform CRUD queries on the stored data. The project demonstrates a complete CI/CD pipeline with GitHub Actions. A pre-built binary artifact is available through the GitHub Actions workflow, which can be downloaded from the GitHub Actions Artifacts. For details about how to use the tool, follow the `User Guide` in this README.

## Requirements/Deliverables
* Create a data pipeline using Databricks
* Include at least one data source and one data sink
* Databricks notebook or script
* Document or video demonstrating the pipeline

# Deliverables
The project includes the files below:

* `.devcontainer` (with `.devcontainer.json` and `Dockerfile`)
* `.github/workflows` for GitHub Actions that integrates Rust CICD workflow and generates a binary artifact you can execute as CLI.
* the `data` folder has the Titanic 
* In the sqlite folder, you will see the 
* In `test_main.py`, we check if all ETL and SQL queries have ran successfully.
* In `mylib`, there are three scripts:
`extract.py`: extracts a dataset from a URL.
`query.py`: contains functions including join, aggregation, and sorting query operations.
`transform_load.py`: loads the transformed data into a Databricks warehouse table using Python's SQL module.


