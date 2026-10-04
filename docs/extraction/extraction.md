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
| Extraction Start Timestamp | Records when extraction begins. | `--` |
| Extraction End Timestamp | Records when extraction finishes. | `--` |
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

Unexpected changes to required column names, data types, or identifiers will be documented and reviewed before the extraction is accepted.

If a source field changes but can be safely mapped to the documented standardized field, the mapping will be updated and recorded.

If a required field is removed or its type becomes incompatible with the expected structure, the extraction will be flagged for review and will not proceed automatically to the next stage.

---

# 7. Extraction Acceptance and Handover Rules

Define the conditions that determine whether extracted data can proceed to the next stage.

For each rule, document:

- Validation condition
- Acceptance criteria
- Pipeline action
- Automatic or manual decision
- Responsible person or role, if manual approval is required

## Required Decisions

### 1. Automatic Approval

Define the conditions for proceeding automatically.

**Example:** All critical validation checks pass.

### 2. Manual Review

Define the conditions that require review.

**Example:** Non-critical warnings or unexpected record counts.

### 3. Rejection or Failure

Define the conditions that prevent the data from proceeding.

Specify whether the pipeline retries, quarantines the data, or stops.

### 4. Handover

Define what output, metadata, and validation results are passed to the next stage.

## Example: Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
|---|---|---|
| All required tables PASS | APPROVED | Proceed to Transformation |
| One or more non-critical WARNING | MANUAL REVIEW | Hold pipeline |
| One or more critical FAIL | REJECTED | Stop/retry/quarantine |

## Validation Summary Result

| Validation | Result |
|---|---|
| sales table validation | PASS |
| customers table validation | PASS |
| products table validation | WARNING |
| orders table validation | PASS |
| Overall Extraction | MANUAL REVIEW |

---

# 8. Common Extraction Problems and Mitigation

Identify realistic extraction problems that may occur in your selected source system.

For each problem, document:

- Problem description
- Possible cause
- Impact on the extraction process
- Proposed solution or mitigation

## Requirements

- Must be relevant to the selected source.
- Solutions must be technically reasonable.
- Avoid listing generic errors without explaining their impact and handling.

## Expected Output

The documentation must be consistent with the implemented extraction process and provide sufficient information for team member to understand, validate, monitor, and maintain the extraction stage.

The extracted data must be validated and packaged for handover to the next stage: Transformation. Data cleaning, standardization, and business transformations must not be performed unless explicitly required by the extraction process.