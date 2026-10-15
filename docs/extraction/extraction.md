# Stage 1: Data Extraction Documentation

## Objective

The goal of this stage is to extract data from an identified source system and prepare a documented, traceable, and validated dataset for the next stage of the data pipeline.

All documentation must be written using Markdown language.

---

# 1. Data Source and Extraction Specification

Identify and describe the source from which the data will be extracted.

## 1.1 Source System

Paintelligent is the source system and the given CSV file by the client used for the project. It supports the retail operation of Garcia Paint Center and contains the operational data needed for sales, product, inventory, and paint analysis activities.


## 1.2 Source Database or File

- **Database management system:** Supabase PostgreSQL
- **File format:** SQL (`.sql`) database export

## 1.3 Extraction Method

The extraction will be performed specifically on the `paint_analysis_history` table from the Paintelligent Supabase PostgreSQL database. The required data will be exported as an SQL (`.sql`) file using the Supabase PostgreSQL SQL export. The extraction will be performed whenever a new dataset is needed for the ETL pipeline.

## 1.4 Extraction Scope

| Source Table / Data Area | Purpose | Extraction |
|---|---|---|
| Product (CSV file) | Contains client-provided paint product information. Product Unit Purchase Price, Estimated Price (PHP), Stocks, Category, and Brand are used in the ETL pipeline. | Required |
| Sales (CSV file) | Contains client-provided sales transaction data, including brand, product, category, sales date, quantity, unit sold, and season for use in the ETL pipeline. | Required |
| Paint Analysis (`paint_analysis_history`) | Contains paint analysis information such as color, RGB, paint components, and analysis date. | Required |

## 1.5 Source Limitations and Assumptions

- The extraction can only be performed when the Paintelligent database is accessible.
- The selected tables and necessary fields are expected to remain available during the extraction.
- Changes to the database structure may require adjustments to the extraction process.
- The extraction time may vary depending on the amount of data stored in the database.
- The exported SQL file represents the available data at the time the extraction is performed.

---

# 2. Source Tables and Column Specification

| Name | Purpose | Important Columns and Data Types | Primary Key / Unique Identifier | Relevant Relationships | Reason for Inclusion |
|---|---|---|---|---|---|
| Product (CSV file) | Contains client-provided paint product information for use in the ETL pipeline. | Product ID; Product Name; Unit Purchase Price; Estimated Price (PHP); Stocks; Category; Brand | N/A | Product information is associated with sales records through the product information in the Sales CSV file. | Required to provide product reference information for processing and integration in the ETL pipeline. |
| Sales (CSV file) | Contains client-provided sales transaction data for use in the ETL pipeline. | Brand; Product; Category; Sales Date; Quantity; Unit Sold; Season | N/A | Product information may be associated with the Product CSV through the product information in the sales records. | Required to provide sales transaction data for processing and analysis in the ETL pipeline. |
| Paint Analysis (`paint_analysis_history`) | Contains paint analysis information retrieved from the Paintelligent Supabase PostgreSQL database. | Id – String; Color – String; RGB – String/JSON; Paint Components – String/JSON; Analysis Date – Date/DateTime | Id | No relevant relationship to the 2 CSV files. | Required to provide paint analysis data for integration and transformation in the ETL pipeline. |

---

# 3. Extraction Validation and Data Quality Checks

## 3.1 Validation Checks

| Check Name | Target | Purpose | Validation Criteria |
|---|---|---|---|
| Source Accessibility | Paintelligent Supabase PostgreSQL database | Verify that the source database is accessible before extraction. | The database connection is established successfully and the required source table can be accessed. |
| File Availability | Product CSV file and Sales CSV file | Verify that the client-provided files required by the ETL pipeline are available. | Both CSV files are present and can be opened and read successfully. |
| Required Table Exists | `paint_analysis_history` | Verify that the selected database table is available for extraction. | The `paint_analysis_history` table exists and can be queried successfully. |
| Required Columns Exist | `paint_analysis_history` | Verify that all required fields are available. | The documented fields, including id, color, RGB, paint components, and analysis date, are present. |
| Required CSV Columns Exist | Product CSV and Sales CSV | Verify that the required fields in the client-provided files are available. | All documented columns required by the ETL pipeline are present in the corresponding CSV file. |
| Identifier Presence | `paint_analysis_history` | Verify that each paint analysis record has its identifier. | The id field is present and is not null for extracted records. |
| Identifier Uniqueness | `paint_analysis_history` | Verify that the identifier uniquely distinguishes extracted records. | No duplicate values are found in the id field. |
| Record Count | All extracted data | Verify that records read from the source are accounted for in the extraction. | The number of records read matches the number of records successfully extracted, except for documented rejected records. |
| Extraction Completeness | `paint_analysis_history` | Verify that the extracted data contains the available source records. | All records available from the selected table at extraction time are included. |
| Extraction Errors | Extraction process | Identify failures or interruptions that could affect the extracted data. | The extraction completes without critical errors; any errors are documented in the extraction log. |
| Output File Validation | SQL (`.sql`) export | Verify that the database export was successfully generated. | The SQL file exists, is accessible, and contains the extracted `paint_analysis_history` data. |

---

# 4. Extraction Metadata and Log Specification

Extraction metadata and logs will be recorded for each extraction run to support monitoring, troubleshooting, auditing, and recovery. The metadata will identify the source, timing, outcome, record counts, and validation status of each run.

| Field Name | Purpose | Example Value / Format |
|---|---|---|
| Pipeline Run ID | Uniquely identifies each extraction run. | `RUN-2026-001` |
| Table or File Name | Identifies the source data processed. | `paint_analysis_history / Product.csv / Sales.csv` |
| Extraction Start Timestamp | Records when extraction begins. | `starts at:` |
| Extraction End Timestamp | Records when extraction finishes. | `ends at:` |
| Extraction Status | Shows whether the extraction completed successfully. | `SUCCESS / FAILED` |
| Number of Records Read | Records available from the source. | `1,250` |
| Number of Records Extracted | Records successfully included in the output. | `1,250` |
| Number of Records Rejected | Records not included, if applicable. | `0` |
| Extraction Window or Data Range | Documents the data period when applicable. | `Full available table / N/A for CSV files` |
| Validation Results | Records the outcome of extraction validation. | `PASS` |
| Error Message or Error Code | Documents errors when applicable. | `N/A` |
| Extraction Duration | Records the total extraction time. | `--` |

The metadata and logs will be used to monitor extraction runs, identify failed or incomplete extractions, support troubleshooting, provide an audit trail, and assist in recovery or reruns when an extraction does not complete successfully.

---

# 5. Log Retention and Access

Extraction logs will be retained for at least 30 days after each extraction run. This period provides sufficient time to review recent extraction activity, troubleshoot issues, and verify the handover to the Transformation Stage.

| Requirement | Specification |
|---|---|
| Log Retention Period | 30 days minimum |
| Justification | Allows recent extraction runs to be reviewed for troubleshooting, auditing, and recovery. |
| Storage Location | Project-controlled extraction log storage associated with the ETL pipeline. |
| Access Permissions | Access limited to authorized project team members responsible for the ETL pipeline. |
| Archiving Requirements | Logs needed for an ongoing investigation or audit will be retained beyond the standard retention period. |
| Deletion Rules | Logs may be deleted after the retention period only when they are no longer required for troubleshooting, auditing, or recovery. |
| Responsible Process | The designated ETL or project administrator is responsible for retention and deletion activities. |

Operational extraction logs will be retained according to the standard retention period, while audit records that are required for longer-term review may be retained for a longer period as needed.

---

# 6. Extraction Data Contract and Naming Convention

## 6.1 Required Schema

| Source | Expected Fields | Data Types / Constraints |
|---|---|---|
| Product CSV | Product ID; Product Name; Unit Purchase Price; Estimated Price (PHP); Stocks; Category; Brand | String; String; Decimal; Decimal; Integer; String; String |
| Sales CSV | Sales ID; Brand; Product; Category; Sales Date; Quantity; Unit Sold; Season | String; String; String; String; Date; Integer; Decimal; String |
| `paint_analysis_history` | Id; Color; RGB; Paint Components; Analysis Date | String; String; String/JSON; String/JSON; Date/DateTime |

## 6.2 Naming Convention

| Naming Rule | Specification |
|---|---|
| Column naming format | Use lowercase for standardized column names. |
| Abbreviations | Avoid unnecessary abbreviations; use clear and descriptive names. |
| Naming consistency | The same business field should use the same standardized name throughout the pipeline. |
| Primary key naming | Use a descriptive identifier name such as `id` or a source-specific identifier such as `product_id` or `sales_id`. |
| Foreign key naming | Use the referenced entity name followed by `_id` where a foreign-key relationship is established. |

## 6.3 Source-to-Standardized Field Mapping

| Source Column | Standardized Column | Source Type | Target Type | Mapping |
|---|---|---|---|---|
| Product CSV - Product ID | `product_id` | String | VARCHAR | Rename |
| Product CSV - Product Name | `product_name` | String | VARCHAR | Rename |
| Product CSV - Unit Purchase Price | `unit_purchase_price` | Decimal | DECIMAL | Rename |
| Product CSV - Estimated Price (PHP) | `estimated_price_php` | Decimal | DECIMAL | Rename |
| Product CSV - Stocks | `stocks` | Integer | INTEGER | Rename |
| Product CSV - Category | `category` | String | VARCHAR | Rename |
| Product CSV - Brand | `brand` | String | VARCHAR | Rename |
| Sales CSV - Sales ID | `sales_id` | String | VARCHAR | Rename |
| Sales CSV - Brand | `brand` | String | VARCHAR | Rename |
| Sales CSV - Product | `product` | String | VARCHAR | Rename |
| Sales CSV - Category | `category` | String | VARCHAR | Rename |
| Sales CSV - Sales Date | `sales_date` | Date | DATE | Rename |
| Sales CSV - Quantity | `quantity` | Integer | INTEGER | Rename |
| Sales CSV - Unit Sold | `unit_sold` | Decimal | DECIMAL | Rename |
| Sales CSV - Season | `season` | String | VARCHAR | Rename |
| `paint_analysis_history` - Id | `id` | String | VARCHAR | No change |
| `paint_analysis_history` - Color | `color` | String | VARCHAR | No change |
| `paint_analysis_history` - RGB | `rgb` | String/JSON | VARCHAR/JSON | Type mapping |
| `paint_analysis_history` - Paint Components | `paint_components` | String/JSON | VARCHAR/JSON | Rename |
| `paint_analysis_history` - Analysis Date | `analysis_date` | Date/DateTime | TIMESTAMP | Rename/Type mapping |

## 6.4 Schema Consistency

Schema consistency ensures that the extracted data follows the expected structure before it moves to the next stage.

- The required column names, data types, and identifiers must match the documented schema.
- Any unexpected changes in the source data structure must be recorded and reviewed.
- If a column name changes but the data can still be mapped correctly, the mapping must be updated and documented.
- If a required column is missing or has an incompatible data type, the extraction must be flagged for review.
- The extracted data must not proceed to the Transformation Stage until critical schema issues are resolved.

---

# 7. Extraction Acceptance and Handover Rules

The extraction stage must check the extracted data before passing it to the Transformation Stage. These rules help ensure that the data is complete, readable, and ready for the next process.

## 7.1 Automatic Approval

The extracted data can proceed automatically when all critical validation checks pass.

- The Product CSV and Sales CSV files are available and readable.
- The paint_analysis_history table is accessible in Supabase PostgreSQL.
- All required columns are present and follow the expected schema.
- The identifiers in the paint analysis records are present and unique.
- The extracted record counts are correct.
- The output files are generated successfully.
- No critical extraction errors are found.

When all these conditions are met, the extraction status will be marked as APPROVED, and the data can proceed to the Transformation Stage.

## 7.2 Manual Review

Manual review is required when the extraction has warnings that do not immediately prevent the process from continuing.

Examples include:

- The number of extracted records is different from the expected count.
- Some records contain missing values in non-critical fields.
- Unexpected columns are found in the source files.
- The source data contains unusual values that need to be checked.
- The extraction log contains warnings that require further investigation.

The pipeline must hold the affected data until an authorized team member reviews the issue. The reviewer must document the findings and decide whether the data can be accepted or must be extracted again.

## 7.3 Rejection or Failure

The extracted data must be rejected when a critical issue prevents it from meeting the required conditions.

Examples include:

- The Supabase database cannot be accessed.
- A required CSV file is missing or cannot be read.
- The paint_analysis_history table cannot be accessed.
- A required column is missing or has an incompatible data type.
- A required identifier is missing or duplicated.
- The output file is incomplete or cannot be generated.
- The extraction process stops before all required data is extracted.

When a failure occurs, the pipeline must stop the handover process and record the error in the extraction log. The team must resolve the problem and rerun the extraction when necessary. Failed or incomplete data must not be passed to the Transformation Stage.

## 7.4 Handover to the Transformation Stage

After the extraction is approved, the pipeline must prepare the extracted data and its supporting records for the next stage.

The handover must include:

- *Extracted data:* The Product CSV, Sales CSV, and exported SQL file containing the paint_analysis_history data.
- *Extraction metadata:* The pipeline run ID, source names, start and end timestamps, extraction status, and record counts.
- *Validation results:* The results of the required checks, including schema, identifiers, record counts, and output file availability.
- *Extraction logs:* Information about successful operations, warnings, errors, and any actions taken to resolve problems.
- *Handover status:* The final approval decision indicating whether the data can proceed to the Transformation Stage.

The receiving stage must confirm that the required files and documentation are available. Data cleaning, standardization, and business-related changes will be handled during the Transformation Stage.

## 7.5 Extraction Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
|---|---|---|
| All critical validation checks pass | APPROVED | Proceed to Transformation |
| Non-critical warnings are found | MANUAL REVIEW | Hold the affected data for review |
| A critical validation check fails | REJECTED | Stop the handover and resolve the issue |
| Extraction is incomplete | FAILED | Record the error and rerun when appropriate |

## 7.6 Validation Summary

The validation summary will record the results of the extraction checks for each source.

| Validation Check | Expected Result |
|---|---|
| Product CSV validation | PASS if the file is readable and required columns are present |
| Sales CSV validation | PASS if the file is readable and required columns are present |
| paint_analysis_history validation | PASS if the table is accessible and required columns are present |
| Identifier validation | PASS if required identifiers are present and unique |
| Record count validation | PASS if the extracted records are properly accounted for |
| Output file validation | PASS if the required output is generated and readable |
| Overall Extraction | APPROVED only when all critical checks pass |

The actual validation results must be recorded after the extraction process runs. The pipeline must not mark the extraction as approved until the required checks have been completed.

---

# 8. Common Extraction Problems and Mitigation

The extraction process may encounter problems when reading CSV files or accessing the Paintelligent Supabase PostgreSQL database. These problems must be recorded and handled properly to protect the quality and completeness of the extracted data.

## 8.1 Database Connection Failure

*Problem Description:* The extraction process cannot connect to the Supabase PostgreSQL database.

*Possible Cause:* The database is unavailable, the connection settings are incorrect, or the network connection is interrupted.

*Impact:* The paint_analysis_history data cannot be extracted, which may prevent the pipeline from completing.

*Proposed Solution:* Check the database connection settings, credentials, network connection, and database availability. Retry the connection when appropriate and record the error in the extraction log.

## 8.2 Missing or Unavailable CSV Files

*Problem Description:* The Product CSV or Sales CSV file cannot be found or opened.

*Possible Cause:* The file is missing, has been renamed, or is stored in the wrong folder.

*Impact:* The pipeline cannot extract the required product or sales data.

*Proposed Solution:* Check the expected file location and confirm that the correct file name is used. Restore or provide the missing file before rerunning the extraction.

## 8.3 Incorrect CSV Format or Encoding

*Problem Description:* A CSV file cannot be read correctly because its format or character encoding is not supported by the extraction process.

*Possible Cause:* The file uses an unexpected delimiter, encoding, or structure.

*Impact:* The extraction may fail, or some values may be read incorrectly.

*Proposed Solution:* Check the CSV format, delimiter, and encoding. Update the file-reading settings when necessary and verify that the data can be read correctly before accepting the extraction.

## 8.4 Missing or Changed Columns

*Problem Description:* A required column is missing, renamed, or changed in the source file or database table.

*Possible Cause:* The source data structure was updated without updating the extraction documentation or process.

*Impact:* The extraction may fail or produce data that does not match the expected schema.

*Proposed Solution:* Compare the source columns with the documented schema. Update the mapping and documentation if the change is valid. If a required field is missing or incompatible, hold the extraction for review.

## 8.5 Duplicate or Missing Identifiers

*Problem Description:* Some records in paint_analysis_history have missing or duplicate identifiers.

*Possible Cause:* The source data contains incomplete records or duplicate entries.

*Impact:* Records may be difficult to identify and track correctly during later processing.

*Proposed Solution:* Check the affected identifiers and record the validation results. Report the issue for review and do not approve the extraction until the identifier problem has been resolved or formally accepted under the project's rules.

## 8.6 Incomplete Data Extraction

*Problem Description:* The number of records extracted does not match the number of records available from the source.

*Possible Cause:* The extraction was interrupted, a query failed, or some records were not included in the output.

*Impact:* The dataset may be incomplete and may not represent all available source records.

*Proposed Solution:* Compare the source and extracted record counts. Check the extraction logs for errors and rerun the extraction when necessary. Approve the data only after the missing records have been accounted for.

## 8.7 Export File Generation Failure

*Problem Description:* The SQL export file is missing, incomplete, or cannot be read.

*Possible Cause:* The export process failed, the database connection was interrupted, or the file was not saved correctly.

*Impact:* The paint analysis data cannot be handed over reliably to the next stage.

*Proposed Solution:* Check the export process and destination folder. Generate the SQL export again and verify that the file exists, can be accessed, and contains the required paint_analysis_history data.

## 8.8 Extraction Process Interruption

*Problem Description:* The extraction stops before all required sources have been processed.

*Possible Cause:* A program error, unexpected system shutdown, or connection interruption occurs during extraction.

*Impact:* Some required datasets may be missing, and the extraction run may be incomplete.

*Proposed Solution:* Record the failure in the extraction log, identify the source where the process stopped, and resolve the cause. Rerun the extraction and perform the required validation checks before handing over the data.

## 8.9 Logging Failure

*Problem Description:* The extraction process completes, but the expected log information is missing or incomplete.

*Possible Cause:* The log file cannot be written, the destination folder is unavailable, or the logging process encounters an error.

*Impact:* The team may have difficulty checking extraction results, identifying problems, or tracking previous runs.

*Proposed Solution:* Check the log file path and write permissions. Make sure that the extraction process records its status, record counts, timestamps, validation results, and errors. If logging fails, report the issue and restore the logging process before treating the run as fully documented.

## 8.10 General Error Handling Rules

To keep the extraction process reliable, the following rules must be followed:

- Record extraction errors and warnings in the log file.
- Do not approve incomplete or invalid output.
- Retry failed operations only when the cause may be temporary or has been resolved.
- Check the extracted data again after rerunning the process.
- Keep the original source data unchanged during extraction.
- Do not perform data cleaning or business transformations during this stage.
- Pass only approved data and its required documentation to the Transformation Stage.

The extraction stage is complete when all required sources have been processed, the output has passed the necessary validation checks, and the data and supporting records are ready for handover.