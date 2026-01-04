# Specialization Project

Title: Supporting Method Selection for Machine
Learning in Product Development and
Production

## Project Overview

This repository contains the implementation used in a university project that analyzes how machine learning is applied in product development and production, based on large-scale analysis of scientific article abstracts.

The study uses an exploratory methodology and treats abstracts as a consistent and scalable unit of analysis. A combination of rule-based methods, weakly supervised text classification, semantic embeddings, and unsupervised clustering is used to characterize machine learning applications across multiple dimensions, including learning paradigms, product lifecycle phases, application contexts, and mentioned ML methods.

The repository documents the full workflow from data collection and preprocessing to classification and analysis.

## Repository structure

The `literature-mining` directory contains the full project implementation.

```
literature-mining/
├── data/                                    # Retrieved and processed bibliographic data
├── fake_data/                               # Small synthetic datasets for testing and development
├── keywords/                                # Keyword lists used for ML paradigm classification
├── ml_methods/                              # Dictionaries for ML method extraction
├── phases/                                  # Textual descriptions used for PLC phase classification
├── queries/                                 # Scopus search queries used for data retrieval
├── reports/                                 # Generated figures, tables, and analysis outputs
└── scripts/                                 # All scripts, notebooks, and experimental pipelines
    ├── analysis/                            # General analysis and exploration
    ├── clustering/                          # UMAP + K-means clustering experiments and pipelines
    ├── cross_dimensional_analyses/          # Analyses combining multiple classification dimensions
    ├── general_abstract_analyses/           # Abstract analysis (length etc.)
    ├── ml_methods_extraction/               # Extraction of mentioned ML methods from abstracts
    ├── ml_paradigm_classification.ipynb     # ML paradigm classification pipelines
    ├── plc_classification/                  # Product lifecycle (PLC) phase classification
    ├── query_analyses/                      # Analysis of query results (overlap etc.)
    ├── refs_analyses/                       # Analysis of reference metadata
    ├── roadmap/                             # Article recommendation roadmap based on classified literature
    └── run_queries/                         # Scripts for executing Scopus queries and data retrieval
```

**Notes**

- The `scripts/` directory contains both finalized pipelines and exploratory experiments conducted during development.
- The `roadmap/` directory implements the literature-based article recommendation approach described in the thesis.

## Environment Setup

It is recommended to run the project in a virtual environment.

```bash
python -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Data Access

Data is retrieved from the Scopus database using the Scopus API via Pybliometrics.
An active Scopus API key is required and is **not included** in this repository.

The API key must be provided via a `.env` file in the project root, for example:

```env
API_KEY=your_api_key_here
```

## Computational Resources

Large-scale embedding generation and model fine-tuning were performed on the IDUN
GPU cluster at NTNU (NVIDIA A100). Most scripts can be run locally, but some
pipelines may require significant memory or GPU resources for full-scale runs.

## Quick Start

1. Set up the virtual environment (see Environment Setup).
2. Run one of the Scopus query scripts in `scripts/run_queries/` to retrieve abstracts.
3. Run the classification pipelines (ML paradigms, PLC phases, clustering, method extraction).
4. Perform cross-dimensional analyses and generate figures in `reports/`.
