# Seismological Data Integration Tool

This Python tool fetches, processes, and stores seismological data from **USGS (United States Geological Survey)** and **ISC (International Seismological Centre)** into a structured SQLite database. It also allows users to query and analyze the data.

---

## Features

- **Data Fetching**:
  - Fetches earthquake data from USGS and ISC APIs.
  - Supports filtering by date range and minimum magnitude.

- **Data Processing**:
  - Standardizes data from different sources into a consistent format.
  - Handles missing or invalid values.

- **Database Integration**:
  - Stores processed data in a SQLite database.
  - Supports querying the database for analysis.

- **Command-Line Interface**:
  - Easy-to-use command-line interface for fetching and querying data.

---

## Prerequisites

Before using this tool, ensure you have the following installed:

- **Python 3.8 or higher**
- **Required Python Libraries**:
  - `pandas`
  - `numpy`
  - `requests`
  - `sqlite3`
