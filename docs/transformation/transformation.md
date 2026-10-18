# Stage 2: Data Transformation Documentation
## Objective
The goal of this stage is to transform the validated data received from the Extraction Stage into a cleaned, standardized, and business-ready dataset for the next stage of the ETL pipeline.
All documentation must be written using Markdown language.
---
# 1. Transformation Input and Processing Specification
## 1.1 Input Source
The Transformation Stage will receive the validated data handed over from the Extraction Stage. The input consists of the Product CSV file, Sales CSV file, and the SQL export containing the `paint_analysis_history` table from the Paintelligent Supabase PostgreSQL database.
The extracted files will be stored in the ETL pipeline's designated staging area before processing. The Product and Sales CSV files will provide product details and sales transaction records, while the SQL export will provide paint analysis records, including color, RGB values, paint components, and analysis dates.
Each transformation batch will reference the extraction run ID and relevant extraction metadata to maintain traceability. The actual file paths, run IDs, and batch windows will be recorded when the pipeline is implemented and executed.
## 1.2 Batch Processing Method
The transformation process is expected to use Python scripts and the Pandas library to read, clean, standardize, and process the extracted datasets. The SQL export must first be restored or parsed into a processable dataset so that the paint analysis records can be transformed alongside the CSV data.
The pipeline will operate through batch processing whenever a new validated dataset is available from the Extraction Stage. Since the extraction documentation does not specify a fixed schedule, the transformation frequency will follow the extraction schedule or be triggered manually when a new batch is ready.
Each batch will process the available extracted records for Product, Sales, and Paint Analysis. If a batch fails because of invalid data, file access problems, or processing errors, the pipeline will record the error and stop the affected batch until the issue is resolved. The failed batch can then be rerun using the same extracted input.
The transformation process should be designed to be idempotent, meaning that rerunning the same batch will produce the same results without creating duplicate transformed records. This will require consistent transformation rules and proper batch tracking.
## 1.3 Transformation Scope
The transformation stage will process the following datasets:
| Input Dataset | Planned Transformation |
|---|---|
| Product CSV | Standardize column names, clean text fields, validate product prices and stock quantities, and convert values to the appropriate data types. |
| Sales CSV | Standardize column names, clean product and brand information, validate sales dates and quantities, and convert numeric and date fields to the appropriate data types. |
| Paint Analysis SQL Export | Process the `paint_analysis_history` records, standardize field names, validate identifiers and dates, and normalize RGB values and paint component data where necessary. |
The main transformation activities will include removing unnecessary whitespace, handling missing or invalid values, checking duplicate records, standardizing column names using lowercase `snake_case`, and converting values into the expected data types.
Business transformations may also include connecting Sales records with Product information using appropriate product identifiers or matching product names, brands, and categories. Additional derived fields and aggregated sales summaries may be created to support sales reporting and future analysis.
The transformation stage will preserve the original extracted data and record the number of records cleaned, modified, rejected, or retained. Records that cannot be safely transformed will be documented and placed in a quarantine area for review rather than being silently discarded.
## 1.4 Dependencies and Assumptions
The transformation process will depend on the successful completion and approval of the Extraction Stage. All required files and records must be available in the staging area before processing begins.
The Product and Sales datasets will be processed before creating combined sales outputs because product information may be needed to validate or enrich sales records. The Paint Analysis dataset will be transformed separately because the extraction documentation identifies no direct relationship between it and the two CSV datasets.
The following assumptions will guide the transformation process:
- The extracted files and SQL export follow the documented extraction data contract.
- Required columns and identifiers remain available and can be mapped to standardized column names.
- Product and Sales records can be matched only when reliable identifiers or sufficiently consistent product information are available.
- RGB values and paint components may require additional parsing or validation depending on their actual stored formats.
- Missing values, duplicate records, invalid dates, and incompatible data types will be handled according to documented transformation rules.
- If a required file, column, or dependency is unavailable, the affected batch will be flagged and prevented from proceeding until the problem is resolved.
- The Transformation Stage will produce validated, standardized datasets for the Load Stage without loading them into the target database or data warehouse.
# 2. Data Profiling and Issue Identification
Data profiling will be performed on the Product CSV, Sales CSV, and `paint_analysis_history` dataset before applying any cleaning or transformation rules. The purpose is to identify missing values, duplicate records, inconsistent formats, invalid values, and other data quality issues that may affect the accuracy of the transformed data.
The profiling process will examine the completeness, consistency, validity, and uniqueness of important fields. The results will be recorded to determine which issues require cleaning, standardization, or further investigation.
Since the actual profiling results have not yet been confirmed, the following table identifies potential issues to investigate. The affected record counts, percentages, and sample values will be updated after the profiling scripts are executed.
## 2.1 Potential Data Quality Issues
| Table or File | Column | Issue Type | Example to Check | Affected Records | Planned Treatment |
|---|---|---|---|---|---|
| Product CSV | `product_name` | Missing values or extra whitespace | Empty value or `" Paint A "` | To be determined | Clean whitespace and review missing names (Rule C-01) |
| Product CSV | `unit_purchase_price` | Invalid numeric values | Text instead of a decimal or a negative price | To be determined | Convert valid values and flag invalid prices (Rule C-02, S-01) |
| Product CSV | `estimated_price_php` | Invalid numeric values | Non-numeric value or negative price | To be determined | Validate and standardize prices (Rule C-02, S-01) |
| Product CSV | `stocks` | Invalid or out-of-range values | Decimal, text, or negative stock | To be determined | Validate integer values and investigate invalid stock (Rule C-03, S-02) |
| Product CSV | `brand`, `category` | Inconsistent text formatting | `"Brand A"` and `" brand a "` | To be determined | Trim whitespace and standardize matching values (Rule C-01, S-03) |
| Sales CSV | `sales_date` | Inconsistent date formats or invalid dates | Different date formats or an unparseable date | To be determined | Convert valid dates to a standard format (Rule S-04) |
| Sales CSV | `quantity` | Invalid numeric values | Text, decimal quantity, or negative quantity | To be determined | Validate quantity and flag invalid records (Rule C-03, S-02) |
| Sales CSV | `unit_sold` | Invalid numeric values | Text instead of a decimal or a negative value | To be determined | Convert valid values and investigate invalid amounts (Rule C-02, S-01) |
| Sales CSV | `product`, `brand`, `category` | Inconsistent text or unmatched product information | Different spellings or extra spaces | To be determined | Standardize text and investigate unmatched products (Rule C-01, B-01) |
| Sales CSV | `sales_id` | Missing or duplicate identifiers | Empty ID or repeated ID | To be determined | Check uniqueness and investigate duplicate records (Rule C-04) |
| Paint Analysis | `id` | Missing or duplicate identifiers | Null ID or repeated ID | To be determined | Reject or quarantine records with invalid identifiers (Rule C-04) |
| Paint Analysis | `color` | Missing values or inconsistent formatting | Empty color name or extra spaces | To be determined | Clean text and review missing color information (Rule C-01) |
| Paint Analysis | `rgb` | Invalid or inconsistent RGB structure | Malformed JSON or RGB values outside the expected range | To be determined | Validate and normalize the RGB representation (Rule S-05) |
| Paint Analysis | `paint_components` | Invalid JSON or inconsistent structure | Malformed JSON or unexpected component format | To be determined | Parse and validate the component data (Rule S-05) |
| Paint Analysis | `analysis_date` | Invalid date or timestamp format | Unparseable date or timestamp | To be determined | Convert valid values to the standard timestamp format (Rule S-04) |
## 2.2 Profiling Procedure
The following steps will be performed to identify data quality issues:
1. **Check missing values:** Count null, empty, and whitespace-only values in important columns.
2. **Check duplicate records:** Identify repeated identifiers and possible duplicate records based on the available fields.
3. **Check data types:** Verify whether prices, stock quantities, sales quantities, dates, and other fields match their expected data types.
4. **Check value validity:** Identify negative prices, invalid stock quantities, malformed RGB values, and other values that violate the expected data rules.
5. **Check text consistency:** Identify extra spaces, inconsistent capitalization, and variations in product names, brands, and categories.
6. **Check relationships:** Determine whether Sales records can be matched to the appropriate Product records using reliable identifiers or consistent product information.
7. **Record profiling results:** Document the actual examples, affected record counts, percentages, and planned treatments for each confirmed issue.
## 2.3 Profiling Results and Issue Tracking
After profiling is completed, the potential issues listed above will be reviewed and updated using the actual results. Issues that are not found will be marked as having no detected occurrences, while newly discovered issues will be added to the table.
The number and percentage of affected records will be calculated from the profiling results. Each confirmed issue will be connected to a cleaning, standardization, or business transformation rule in the following sections.
Records that cannot be safely corrected will be flagged for review or placed in a quarantine area. No records will be silently removed during the transformation process, and all rejected records will be counted and documented in the transformation logs.
## 3. Data Cleaning Rules
### 3.1 Data Cleaning Overview
The data cleaning stage prepares the Product, Sales, and Paint Analysis datasets for transformation. It identifies and handles poor-quality data through defined validation and cleaning rules. Valid records will be retained, while records containing obvious errors or clearly invalid values will be corrected when possible or placed in the quarantine table when they cannot be processed reliably.
### 3.2 Data Cleaning Rules
| Rule ID and Name | Target Table and Column | Condition | Action | Justification |
|---|---|---|---|---|
| C-01: Whitespace Cleaning | Product: `product_id`, `product_name`, `category`, `brand`; Sales: `brand`, `product`, `category`, `season`; Paint Analysis: `id`, `color`, `rgb`, `paint_components` | Text values contain leading or trailing whitespace. | Remove unnecessary leading and trailing whitespace while preserving the remaining value. | Maintains consistent values and improves matching between datasets. |
| C-02: Missing or Null Value Handling | Product: `product_id`, `product_name`; Sales: `sales_id` if available; Paint Analysis: `id` | An identifying field required for processing is null or empty. | Retain the record if it can still be processed reliably. Otherwise, quarantine it without inserting an arbitrary default value. | Prevents incomplete records from causing processing errors or incorrect results. |
| C-03: Numeric Value Validation | Product: `unit_purchase_price`, `estimated_price_php`, `stocks`; Sales: `quantity`, `unit_sold` | A numeric value cannot be converted to the appropriate type or violates an applicable business rule. | Convert valid values to the appropriate numeric type. Quarantine clearly invalid values that cannot be corrected reliably. | Ensures numeric data can be processed consistently and accurately. |
| C-04: Duplicate Record Detection | Product: `product_id`; Sales: `sales_id` if available; Paint Analysis: `id` | Multiple records share an identifier expected to be unique. | Compare duplicate records. Retain the verified correct record when it can be determined. Quarantine unresolved unintended duplicates and record the reason. | Prevents duplicate records from affecting counts and analysis without silently discarding data. |
| C-05: Product Reference Validation | Sales: `product`, `brand`, `category`; Product: `product_name`, `brand`, `category` | A Sales record cannot be matched to a corresponding product in the Product dataset. | Check normalized values before matching. Quarantine confirmed unmatched records that cannot be reliably linked to a product. | Maintains consistency between Sales and Product data and prevents unreliable product-level analysis. |
| C-06: Date Value Validation | Sales: `sales_date`; Paint Analysis: `analysis_date` | A date value is invalid or cannot be parsed. | Convert valid dates to the appropriate date or datetime type. Quarantine invalid dates that cannot be corrected reliably. | Supports accurate date-based processing and analysis. |
| C-07: RGB Value Validation | Paint Analysis: `rgb` | RGB data is malformed or contains channel values outside the range of 0–255. | Validate the RGB structure and channel values. Quarantine clearly invalid RGB data. | Ensures color information remains valid for paint analysis. |
| C-08: Paint Component Validation | Paint Analysis: `paint_components` | Paint component data is malformed or cannot be parsed according to its expected structure. | Preserve valid component data and quarantine malformed data that cannot be processed reliably. | Ensures paint component information is usable during transformation. |
| C-09: Invalid Character Handling | Applicable text fields in Product, Sales, and Paint Analysis | A character or value violates the field's expected format and prevents reliable processing. | Remove or correct the invalid character only when the intended value is clear. Otherwise, quarantine the affected record. | Prevents invalid formatting from interfering with processing while preserving legitimate values. |
### 3.3 Missing Values and Duplicate Handling
Missing values will be evaluated according to the role of the affected field. No arbitrary default values will be inserted. Records with missing values may be retained when the missing information does not prevent processing. Records that cannot be processed reliably because of a missing required value will be quarantined.
Duplicate detection will use the available identifiers: `product_id` for Product, `sales_id` for Sales if present, and `id` for Paint Analysis. When duplicate records are found, the pipeline will compare their values and retain the record that can be verified as correct. If the correct record cannot be determined, the unresolved duplicate records will be quarantined for review rather than silently removed.
### 3.4 Data Cleaning Procedures
The pipeline will follow these procedures:
1. **Load Extracted Data:** Load the Product and Sales CSV files and prepare the Paint Analysis SQL export for processing.
2. **Preserve Source Data:** Perform cleaning on staging copies without modifying the original extracted files.
3. **Clean Text Values:** Remove unnecessary leading and trailing whitespace from applicable fields.
4. **Handle Missing Values:** Check identifying fields and determine whether records can still be processed reliably.
5. **Validate Numeric Values:** Convert valid numeric values and check applicable business constraints.
6. **Detect Duplicates:** Identify duplicate identifiers and determine which records can be reliably retained.
7. **Validate Product References:** Check Sales records against the Product dataset.
8. **Validate Dates:** Convert valid dates and identify invalid date values.
9. **Validate Paint Data:** Check RGB values and the structure of paint component data.
10. **Handle Invalid Characters:** Correct invalid characters only when the intended value is clear.
11. **Quarantine Invalid Records:** Store records that cannot be processed reliably and record the applicable rule ID and reason.
12. **Record Results:** Count and log records processed, retained, modified, and quarantined.
13. **Save Cleaned Data:** Store the cleaned datasets for the next ETL stage.
### 3.5 Quarantine Table
The quarantine table stores records that contain obvious errors or clearly invalid values that cannot be reliably corrected during cleaning.
| Field | Description |
|---|---|
| Batch ID | Identifies the ETL batch in which the issue was detected. |
| Source Dataset | Identifies Product, Sales, or Paint Analysis as the source. |
| Original Record | Preserves the original record values for review. |
| Rule ID | Identifies the cleaning rule that was triggered. |
| Quarantine Reason | Describes the error or reason the record could not proceed. |
| Quarantine Timestamp | Records when the issue was identified. |
Every record removed from the main processing flow or rejected during cleaning will be counted and logged. Corrected records will be tracked as modified records, while quarantined records will remain available for investigation. Original extracted data will be preserved.
### 3.6 Cleaning Logs and Expected Output
The cleaning logs will record the ETL batch, source dataset, applicable rule, number of records processed, number retained, number modified, and number quarantined. Quarantine reasons will also be documented to support monitoring and troubleshooting.
The expected output consists of cleaned Product, Sales, and Paint Analysis datasets, a quarantine table for records requiring review, and cleaning logs documenting the results. These outputs will be used in the subsequent transformation stage.
## 4. Standardization and Data Type Rules
### 4.1 Standardization Overview
The standardization process ensures that the Product, Sales, and Paint Analysis datasets use consistent column names, data types, and formats. Valid values will be converted to their appropriate types while preserving their original meaning.
### 4.2 Data Type and Format Rules
| Rule ID and Name | Target Column | Standard Format or Type | Action | Justification |
|---|---|---|---|---|
| S-01: Column Name Standardization | All columns in Product, Sales, and Paint Analysis | Lowercase `snake_case` | Rename columns using consistent, descriptive names. | Maintains consistent naming throughout the pipeline. |
| S-02: Text Type Standardization | Product: `product_id`, `product_name`, `category`, `brand`; Sales: `brand`, `product`, `category`, `season`; Paint Analysis: `id`, `color` | String / VARCHAR | Convert applicable text fields to string values while preserving their content. | Supports consistent text processing and matching. |
| S-03: Decimal Type Standardization | Product: `unit_purchase_price`, `estimated_price_php`; Sales: `unit_sold` | Decimal | Convert valid values to a decimal data type without changing their meaning. | Supports consistent processing of decimal values. |
| S-04: Integer Type Standardization | Product: `stocks`; Sales: `quantity` | Integer | Convert valid whole-number values to integers. | Supports accurate quantity and stock calculations. |
| S-05: Date Type Standardization | Sales: `sales_date`; Paint Analysis: `analysis_date` | Date or datetime | Convert valid date values to a consistent format, using `YYYY-MM-DD` for dates and ISO 8601 for datetime values when applicable. | Ensures consistent date processing and comparison. |
| S-06: Paint Data Type Standardization | Paint Analysis: `rgb`, `paint_components` | String or structured JSON, according to the source format | Preserve valid values and parse JSON-formatted data when required for processing. | Maintains the structure of paint analysis information. |
| S-07: Numeric Precision | Decimal fields | Decimal with two fractional digits where appropriate | Preserve the available precision and apply rounding only when required by the target specification. | Prevents unnecessary loss of numeric accuracy. |
### 4.3 Source-to-Standardized Field Mapping
| Source Dataset | Source Column | Standardized Column | Target Type |
|---|---|---|---|
| Product CSV | Product ID | `product_id` | VARCHAR |
| Product CSV | Product Name | `product_name` | VARCHAR |
| Product CSV | Unit Purchase Price | `unit_purchase_price` | DECIMAL |
| Product CSV | Estimated Price (PHP) | `estimated_price_php` | DECIMAL |
| Product CSV | Stocks | `stocks` | INTEGER |
| Product CSV | Category | `category` | VARCHAR |
| Product CSV | Brand | `brand` | VARCHAR |
| Sales CSV | Sales ID, if present | `sales_id` | VARCHAR |
| Sales CSV | Brand | `brand` | VARCHAR |
| Sales CSV | Product | `product` | VARCHAR |
| Sales CSV | Category | `category` | VARCHAR |
| Sales CSV | Sales Date | `sales_date` | DATE |
| Sales CSV | Quantity | `quantity` | INTEGER |
| Sales CSV | Unit Sold | `unit_sold` | DECIMAL |
| Sales CSV | Season | `season` | VARCHAR |
| `paint_analysis_history` | Id | `id` | VARCHAR |
| `paint_analysis_history` | Color | `color` | VARCHAR |
| `paint_analysis_history` | RGB | `rgb` | String or JSON |
| `paint_analysis_history` | Paint Components | `paint_components` | String or JSON |
| `paint_analysis_history` | Analysis Date | `analysis_date` | DATE or DATETIME |
### 4.4 Conversion Failure Handling
Values that cannot be converted to their expected data types will be checked against the cleaning rules in Section 3. If the value cannot be corrected reliably, the affected record will be quarantined and the conversion error will be recorded.
Unexpected source column names, missing required columns, or incompatible data types will trigger schema validation. The transformation will stop or require review when the changes prevent reliable processing.
---
## 5. Business Transformation Rules
### 5.1 Business Transformation Overview
The business transformation stage converts the cleaned and standardized datasets into a consistent structure for downstream processing and analysis. It will use the available Product, Sales, and Paint Analysis data without introducing unsupported calculations or relationships.
### 5.2 Business Transformation Rules
| Rule ID and Name | Input Columns | Logic or Formula | Output Column or Dataset | Business Reason |
|---|---|---|---|---|
| B-01: Product Data Standardization | All Product CSV columns | Apply the cleaning and standardization rules, then preserve the defined Product fields. | Standardized Product dataset | Provides consistent product and inventory information. |
| B-02: Sales Data Standardization | All Sales CSV columns | Apply the cleaning and standardization rules, then preserve the defined Sales fields. | Standardized Sales dataset | Prepares sales records for downstream processing and analysis. |
| B-03: Paint Analysis Standardization | `id`, `color`, `rgb`, `paint_components`, `analysis_date` | Apply the cleaning and standardization rules while preserving paint analysis values. | Standardized Paint Analysis dataset | Maintains consistent paint analysis information for later use. |
| B-04: Product Reference Matching | Sales: `product`, `brand`, `category`; Product: `product_name`, `brand`, `category` | Match Sales records to Product records using the available product information and validated matching criteria. | Matched Sales and Product data, where applicable | Supports consistent product-level analysis. |
| B-05: Output Preparation | Cleaned and standardized datasets | Validate the transformed datasets and prepare them for handover without loading them into the target database. | Transformed datasets and validation results | Ensures the output is prepared for the Load stage. |
### 5.3 Joins, Aggregations, and Filtering
Sales records may be matched to Product records using the available product information. The matching process must verify that the selected fields identify the intended product and do not cause unexpected row multiplication. Unmatched records will be handled according to Section 3.
No additional sales calculations, aggregations, or business exclusions will be applied unless the required business logic and source fields are defined. Paint Analysis will remain a separate dataset because no relationship to the Product or Sales datasets has been established in the extraction documentation.
The transformation stage will prepare the output datasets only. Loading the results into the target database or data warehouse belongs to the Load stage.
---
## 6. Transformed Data Contract and Naming Convention
### 6.1 Required Output Schema
The transformed output will preserve the documented source fields using standardized column names and data types.
| Output Dataset | Output Columns | Data Types | Key or Constraint |
|---|---|---|---|
| `products` | `product_id`, `product_name`, `unit_purchase_price`, `estimated_price_php`, `stocks`, `category`, `brand` | VARCHAR, VARCHAR, DECIMAL, DECIMAL, INTEGER, VARCHAR, VARCHAR | `product_id` is the candidate identifier based on the source field. |
| `sales` | `sales_id` if present, `brand`, `product`, `category`, `sales_date`, `quantity`, `unit_sold`, `season` | VARCHAR if present, VARCHAR, VARCHAR, VARCHAR, DATE, INTEGER, DECIMAL, VARCHAR | Preserve the available sales identifier if it exists in the source. |
| `paint_analysis` | `id`, `color`, `rgb`, `paint_components`, `analysis_date` | VARCHAR, VARCHAR, String/JSON, String/JSON, DATE/DATETIME | `id` is the source identifier. |
The actual nullability and uniqueness constraints will be confirmed against the source data and the cleaning rules. No new surrogate keys or foreign-key columns will be introduced without a defined requirement.
### 6.2 Naming Convention
| Naming Rule | Specification |
|---|---|
| Column names | Use lowercase `snake_case`. |
| Dataset names | Use descriptive names such as `products`, `sales`, and `paint_analysis`. |
| Identifier names | Preserve source identifiers using names such as `product_id`, `sales_id` when present, and `id`. |
| Data type consistency | Use the same data type for equivalent fields throughout the transformation process. |
| Derived columns | Add only when a defined business transformation requires them. |
| Foreign keys | Introduce only when a valid relationship and matching key have been established. |
### 6.3 Source-to-Target Mapping
Each output field will be mapped to its source column or documented transformation rule. Renamed fields will retain their original meaning, while converted fields will use the target data type defined in Section 4.
| Source | Target Dataset | Target Column | Transformation Applied |
|---|---|---|---|
| Product CSV | `products` | `product_id` | Rename and validate type |
| Product CSV | `products` | `product_name` | Rename and clean text |
| Product CSV | `products` | `unit_purchase_price` | Rename and convert to decimal |
| Product CSV | `products` | `estimated_price_php` | Rename and convert to decimal |
| Product CSV | `products` | `stocks` | Rename and convert to integer |
| Product CSV | `products` | `category`, `brand` | Rename and clean text |
| Sales CSV | `sales` | `sales_id`, if present | Rename and preserve identifier |
| Sales CSV | `sales` | `brand`, `product`, `category`, `season` | Rename and clean text |
| Sales CSV | `sales` | `sales_date` | Rename and convert to date |
| Sales CSV | `sales` | `quantity` | Rename and convert to integer |
| Sales CSV | `sales` | `unit_sold` | Rename and convert to decimal |
| `paint_analysis_history` | `paint_analysis` | `id`, `color` | Rename and clean applicable text |
| `paint_analysis_history` | `paint_analysis` | `rgb`, `paint_components` | Preserve or parse according to source format |
| `paint_analysis_history` | `paint_analysis` | `analysis_date` | Rename and convert to date or datetime |
### 6.4 Schema Consistency
Before processing, the pipeline will check that the expected source columns are available. If a column is renamed, removed, or changed to an incompatible type, the transformation will not silently continue using an incorrect mapping.
Changes that can be safely mapped will be documented. Changes that prevent reliable transformation will cause the batch to stop or require manual review. The output schema will remain consistent with the documented contract.
---
## 7. Transformation Validation and Data Quality Checks
### 7.1 Validation Overview
Validation checks will confirm that the transformed datasets are complete, consistent, correctly typed, and suitable for the Load stage. Each check will have defined criteria and a severity level.
### 7.2 Validation Rules
| Check Name | Target | Purpose | Validation Criteria | Severity |
|---|---|---|---|---|
| V-01: Record Reconciliation | Product, Sales, and Paint Analysis datasets | Detect missing or unaccounted-for records. | For row-level cleaning: `records_read = records_output + records_rejected`. Modified and retained records remain part of the output count. | Critical |
| V-02: Required Field Validation | Required identifying fields in all output datasets | Ensure required identifiers are available. | No invalid or missing required identifiers remain in the accepted output. | Critical |
| V-03: Identifier Uniqueness | Product: `product_id`; Paint Analysis: `id`; Sales: `sales_id` if present | Detect unintended duplicate identifiers. | No unresolved duplicate identifiers remain in the accepted output. | Critical |
| V-04: Data Type and Format Validation | All standardized output columns | Confirm that data conforms to the output contract. | All output fields match their documented target types and formats. | Critical |
| V-05: Product Reference Integrity | Sales and Product datasets | Detect unmatched product references. | All records treated as matched have a valid product match; unmatched records are accounted for. | Critical |
| V-06: Date Validation | `sales_date`, `analysis_date` | Confirm date values can be processed correctly. | All accepted date values conform to their expected date or datetime types. | Critical |
| V-07: RGB Validation | Paint Analysis: `rgb` | Confirm RGB data is valid. | RGB values follow the expected structure and channel values fall within 0–255. | Critical |
| V-08: Paint Component Validation | Paint Analysis: `paint_components` | Confirm component data can be processed. | Component data conforms to its expected structure. | Critical |
| V-09: Transformation Status | Entire batch | Determine whether the transformation completed successfully. | Processing completes without unresolved critical errors, and required validation results are recorded. | Critical |
### 7.3 Record Reconciliation
The pipeline will compare the number of records read with the number of output and rejected records.
`records_read = records_output + records_rejected`
Records that are cleaned and retained count as output records, even when their values have been modified. The `records_modified` metric will be tracked separately and will not be added to the reconciliation equation.
For matching and other operations that change row counts, the pipeline will also check join cardinality, unmatched records, and unexpected row multiplication. Any filtering or exclusion must have a documented reason and count.
### 7.4 Validation Results
Each validation check will produce a result of `PASS`, `WARNING`, or `FAIL`.
- **PASS:** The validation criteria are satisfied.
- **WARNING:** A non-critical issue requires review but does not necessarily prevent processing.
- **FAIL:** A critical validation condition is not satisfied and the batch cannot automatically proceed.
The validation results will be recorded in the transformation logs and used to determine whether the output can be handed over to the Load stage.
---
## 8. Transformation Metadata and Log Specification
### 8.1 Metadata Overview
Transformation metadata and logs will record the details of each batch run. They will support monitoring, troubleshooting, auditing, data lineage, and recovery when a transformation fails.
### 8.2 Required Metadata Fields
| Field | Purpose | Example Value or Format |
|---|---|---|
| `run_id` | Identifies the current transformation run. | `RUN_20261018_0800` |
| `parent_run_id` | References the extraction run that supplied the input data. | Existing extraction run ID |
| `batch_id` | Identifies the processed batch. | Batch identifier |
| `batch_window` | Records the data range or date period processed, when applicable. | `2026-10-17 to 2026-10-18` |
| `table_name` | Identifies the dataset being transformed. | `products` |
| `start_timestamp` | Records when transformation started. | ISO 8601 datetime |
| `end_timestamp` | Records when transformation finished. | ISO 8601 datetime |
| `status` | Records the result of the transformation run. | `SUCCESS`, `WARNING`, or `FAILED` |
| `records_read` | Number of records received from the extraction handover. | Integer |
| `records_output` | Number of records produced by transformation. | Integer |
| `records_rejected` | Number of records quarantined or rejected. | Integer |
| `records_modified` | Number of retained records modified during cleaning or transformation. | Integer |
| `rules_applied` | Records which cleaning or transformation rules were applied and their affected record counts. | `C-01: count` |
| `validation_results` | Records the outcomes of validation checks. | `PASS` or `FAIL` per check |
| `error_message` or `error_code` | Documents the error when processing fails. | Error description or code |
| `duration` | Records the total transformation time. | Seconds |
| `code_version` or `rules_version` | Identifies the version of the transformation code or rules used. | Version identifier |
The `parent_run_id` will reference extraction metadata rather than duplicating all extraction details.
### 8.3 Logging and Monitoring
The pipeline will record the start and end of each run, the datasets processed, the number of records read and produced, applied rules, rejected records, validation results, and errors.
These logs will help the team identify failed batches, trace transformed data to its extraction run, verify record counts, investigate data-quality issues, and determine whether a failed batch can be safely rerun.
---
## 9. Log Retention, Rejected Data, and Access
### 9.1 Retention and Management
Transformation logs and quarantined records will be stored separately from the original extracted files and cleaned output datasets. Their retention will support troubleshooting, auditing, and recovery.
| Requirement | Specification |
|---|---|
| Log Retention Period | At least 30 days. |
| Justification | Provides time to review recent transformation runs and investigate processing errors. |
| Log Storage Location | Project-controlled storage associated with the ETL pipeline. |
| Quarantine Storage Location | A designated quarantine table or file managed by the transformation pipeline. |
| Quarantine Retention | Retain quarantined records for at least 30 days and longer when needed for investigation, auditing, or recovery. |
| Read Permissions | Authorized project team members responsible for the ETL pipeline may review logs and quarantined records. |
| Write Permissions | The ETL process and authorized team members may write transformation logs and quarantine records. |
| Delete Permissions | Deletion is restricted to the designated ETL or project administrator. |
| Archiving | Logs and quarantined records needed for ongoing investigations or audits will be retained beyond the standard period. |
| Deletion Rules | Records may be deleted only after the retention period has ended and they are no longer required for troubleshooting, auditing, or recovery. |
### 9.2 Separation of Records
Transformation logs will document cleaning actions, type conversions, validation results, and rejected records. The quarantine table will preserve records that could not be safely transformed, together with their rejection reason, batch ID, and rule ID.
Original extracted data will remain unchanged. Transformation logs and quarantined records will not be deleted before the defined retention period or while they are needed for troubleshooting, auditing, or recovery.
---
## 10. Transformation Acceptance and Handover Rules
### 10.1 Acceptance Overview
The acceptance process determines whether transformed data can proceed to the Load stage. The decision will depend on validation results, record reconciliation, unresolved errors, and the rejection rate.
The rejection-rate threshold must be established before the pipeline is evaluated. For this project, a proposed threshold of **1%** will be used as an initial acceptance limit. This provides a clear limit for review, but the project team should approve the threshold before implementation.
Rejection rate will be calculated as:
`Rejection Rate (%) = (records_rejected / records_read) × 100`
If no records are read, the batch will not be automatically approved based on the rejection rate; it will require investigation.
### 10.2 Acceptance Rules
| Rule ID | Validation Condition | Acceptance Criteria | Pipeline Action | Decision Type | Responsible Role |
|---|---|---|---|---|---|
| A-01: Automatic Approval | All critical validation checks pass, record reconciliation succeeds, and the rejection rate is within the approved threshold. | No unresolved critical failures; rejection rate is at or below 1%. | Mark the batch as approved and prepare the output for handover to Load. | Automatic | ETL pipeline |
| A-02: Manual Review | A non-critical warning occurs, or the rejection rate exceeds 1% while the output remains available for review. | The issue and its impact are documented, and the output is reviewed before acceptance. | Hold the batch until an authorized team member reviews the results. | Manual | Designated ETL or project reviewer |
| A-03: Rejection or Failure | A critical validation check fails, record reconciliation fails, or the output schema is incompatible. | The batch does not meet the critical acceptance criteria. | Stop the handover, record the failure, and investigate whether correction or a controlled rerun is appropriate. | Automatic failure with review as needed | ETL pipeline and designated reviewer |
| A-04: Handover | The batch is approved for the next stage. | Transformed datasets, validation results, record counts, and linked run metadata are available. | Package the outputs and metadata for the Load stage without loading them into the target database. | Automatic after approval | ETL pipeline |
### 10.3 Acceptance Decision
| Overall Condition | Acceptance Decision | Pipeline Action |
|---|---|---|
| All critical checks pass and rejection rate is within the approved threshold. | `APPROVED` | Proceed to Load handover. |
| Non-critical warnings exist or rejection rate exceeds the threshold. | `MANUAL REVIEW` | Hold the batch for review. |
| A critical check fails or reconciliation is unsuccessful. | `REJECTED` | Stop the handover and investigate. |
### 10.4 Handover Requirements
The transformation handover will include:
- Cleaned and standardized Product, Sales, and Paint Analysis datasets.
- Transformation run ID and linked extraction run ID.
- Record counts for input, output, modified, and rejected records.
- Cleaning and transformation rules applied.
- Validation results and the overall acceptance decision.
- Error details and quarantine references, where applicable.
The Load stage may proceed only after the transformed output satisfies the acceptance rules or receives the required manual approval.
---
## Expected Output
The Stage 2: Data Transformation Documentation will describe how the extracted Paintelligent datasets are cleaned, standardized, transformed, validated, and prepared for the Load stage.
The expected deliverables are:
1. **Cleaned datasets:** Product, Sales, and Paint Analysis records that pass the defined cleaning rules.
2. **Standardized datasets:** Data with consistent column names, data types, and formats.
3. **Transformed datasets:** Business-ready output preserving the documented source fields and applying only defined transformations.
4. **Quarantine records:** Records containing errors that cannot be safely corrected, with the applicable rule and reason documented.
5. **Validation results:** Evidence of record reconciliation, data type conformance, identifier checks, product reference checks, and other applicable quality checks.
6. **Transformation metadata and logs:** Batch identifiers, linked extraction run IDs, record counts, rule activity, validation outcomes, and error information.
7. **Acceptance decision:** An `APPROVED`, `MANUAL REVIEW`, or `REJECTED` result based on the documented criteria.
8. **Load handover package:** Approved transformed datasets and their supporting metadata and validation results.
The transformation stage ends when the outputs are validated and packaged for handover. Loading the transformed data into the target database or data warehouse is part of the next ETL stage.