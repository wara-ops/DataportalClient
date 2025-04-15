# DataportalClient
Python client library for the WARA-Ops dataportal API.

**This repository is automatically maintained, and should not be manually updated.**

The client load parts of a selected file from a dataset directly into a pandas dataframe. When loading data, the client will in the background download the file into the `.cache` folder in the user's home directory. This is done to speed up fetching more data from the same file. Currently only one file can be loaded at any time. Loading data from a new file will restart the downloading process and cause the old file to be overwritten.

## Setup

The client uses pdm (https://pdm-project.org/latest/) as its package manager. See the webpage for installation instructions.

Tests and lints can be run with `pdm run test` and `pdm run lint`.


