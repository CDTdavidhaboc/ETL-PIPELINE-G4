# Lab 2 – Technical Metadata Documentation

## 1. Pipeline Metadata

The proposed **Product, Sales and Paint Analysis Pipeline** processes exported data from the Paintelligent Supabase database. Python is used for data ingestion, cleaning, transformation, and ETL orchestration, while SQLite is used for data storage and SQL-based analysis. The pipeline is designed to process exported datasets whenever new files are provided rather than operating continuously. Its main purpose is to convert raw sales, product, and paint analysis data into clean, structured, and analysis-ready information for business decision-making. :contentReference[oaicite:1]{index=1}

## 2. Data Lineage

The data lineage describes how data moves through the proposed ETL pipeline. The process begins with exported sales, product, and paint analysis data from Supabase. The data is ingested using Python, checked and cleaned for quality issues, transformed and standardized, and then loaded into SQLite. SQL queries are used to organize and retrieve the processed information. The final output consists of business-oriented metrics and insights such as top-selling products, slow-moving products, sales trends, and product performance. :contentReference[oaicite:2]{index=2}

### Six-Stage Data Lineage

1. **Source** – Exported sales, product, and paint analysis datasets are prepared from the Paintelligent Supabase database.
2. **Ingestion** – Python reads and imports the raw exported files into the ETL pipeline.
3. **Cleaning** – The data is checked for missing values, duplicates, inconsistent formats, invalid values, and other quality issues.
4. **Transformation** – Data types and values are standardized, required fields are calculated, and records are organized into a consistent structure.
5. **Storage & Querying** – Transformed data is loaded into SQLite tables and queried using SQL for business analysis.
6. **Business Output** – The processed data is used to generate business reports, analytical results, and decision-support insights. :contentReference[oaicite:3]{index=3}

## 3. Schema Metadata

The proposed SQLite database contains three main tables: **product_table**, **sales_table**, and **paint_analysis_table**. The schema defines the columns, data types, primary keys, foreign keys, and relationships needed for business-oriented analysis. The ETL system works separately from the operational Paintelligent database, using exported data as its source and loading the processed information into SQLite. :contentReference[oaicite:4]{index=4}

### Main Tables

- **product_table** – Stores product information such as product ID, name, category, brand, price, cost, stock quantity, and status.
- **sales_table** – Stores sales transactions including sale ID, product ID, sale date, quantity sold, total amount, and aggregated sales.
- **paint_analysis_table** – Stores paint analysis information including analysis ID, product ID, color, finish, components, and analysis date. :contentReference[oaicite:5]{index=5}

### Keys and Relationships

The primary keys are `product_id` for products, `sale_id` for sales, and `analysis_id` for paint analysis. The `product_id` in the Paint Analysis table serves as a foreign key connecting paint analysis records to their corresponding products. The Product and Paint Analysis tables have a **one-to-many relationship**, while multiple paint analysis records can refer to the same product. The Sales table has no direct foreign-key relationship with either Product or Paint Analysis in the proposed schema. :contentReference[oaicite:6]{index=6}

## 4. Technology Justification

**Python** was selected for ETL processing because it can handle data extraction, cleaning, transformation, validation, automation, and analysis. It allows the exported datasets to be processed and prepared for business-oriented use.

**SQLite** was selected for data storage because it is lightweight, simple to deploy, and does not require a separate database server. It provides SQL capabilities for querying and analyzing structured data, supporting outputs such as product performance and sales trends. :contentReference[oaicite:7]{index=7}