# Stage 2: Data Transformation Documentation
    ## Objective
    The purpose of Stage 2 is to convert the extracted Product, Sales, and Paint Analysis datasets into consistent, cleaned, and analysis-ready outputs for the next stage of the Paintelligent ETL pipeline.
    The transformation process uses Python and Pandas. It standardizes source headers through flexible alias mapping, cleans values, validates data types and business rules, generates transformation-stage identifiers where required, and records records that cannot safely pass validation in `quarantine.csv`.
    **Important distinction:** Flexible header mapping addresses differences in column naming. It does not automatically correct invalid values, missing required information, or inconsistent relationships between records. These are handled by the cleaning and validation rules in this document.
    ---
    # 1. Transformation Input and Processing Specification
    ## 1.1 Input Sources
    The Transformation Stage receives three datasets from the Extraction Stage:
    | Input dataset | Content |
    |---|---|
    | Product CSV | Product names, purchase prices, estimated selling prices, stock quantities, categories, and brands. |
    | Sales CSV | Sales dates, products, brands, categories, quantities, sales amounts, and seasons, where available. |
    | Paint Analysis dataset | Records from the `paint_analysis_history` table, including color, RGB values, paint components, and analysis dates. |
    The extraction scripts are loaded from the project directory. The Product and Sales data are returned by `csv_extraction.py`. Paint Analysis data are returned by `analysis_extraction.py`.
    The transformation script writes its outputs to `transformation/output/` and its run log to `logs/transformation.log`. The quarantine output is `transformation/output/quarantine.csv`.
    ## 1.2 Batch Processing
    The script processes the available extracted datasets in one transformation run. It transforms Products first, then Sales using the accepted Product output for product-reference checking, and then Paint Analysis separately.
    Each run creates a run ID and batch ID and records timestamps, record counts, validation results, and processing status. If an exception interrupts processing, the error is written to the transformation log and the available quarantine records are saved.
    The script generates new `product_id` and `sales_id` values during transformation. These identifiers are unique within the generated output for that run, but they change when the script is run again. Therefore, the generated IDs are run-specific identifiers and should not be treated as stable source-system identifiers across reruns.
    ## 1.3 Transformation Scope
    | Dataset | Transformation activities |
    |---|---|
    | Products | Flexible header mapping, text trimming, price conversion, stock conversion, validation of product names and nonnegative numeric values, and generation of `product_id`. |
    | Sales | Flexible header mapping, text trimming, quantity and sales-amount conversion, date parsing, product-reference checking, and generation of `sales_id`. |
    | Paint Analysis | Flexible header mapping, text trimming, RGB validation, JSON-like component validation, analysis-date parsing, and duplicate checking for nonblank source IDs. |
    The script creates standardized output schemas:
    - `products_transformed.csv`
    - `sales_transformed.csv`
    - `paint_analysis_transformed.csv`
    - `quarantine.csv`
    The transformation stage does not load data into the target database. Loading is handled by the next ETL stage.
    ## 1.4 Dependencies and Assumptions
    - Python and Pandas are available.
    - The extraction scripts can be located and loaded.
    - The extraction functions return DataFrames for Products, Sales, and Paint Analysis.
    - Source column names may differ in capitalization, punctuation, spacing, abbreviations, or wording.
    - The flexible header mapper normalizes header names and matches known aliases before applying limited field-specific heuristics.
    - Unknown source columns are logged and preserved during mapping, but only fields in the documented output schema are included in the final output.
    - Product and Sales records are matched using normalized product names. The current matching logic ignores case and leading/trailing whitespace, but it does not perform fuzzy matching.
    - A missing optional value is not automatically an error. A nonblank value that cannot be converted to the required type may be rejected.
    - Records rejected by the current validation rules are written to quarantine with a rule ID and reason.
    ---
    # 2. Flexible Header Mapping and Data Profiling
    ## 2.1 Flexible Header Mapping
    Source systems and spreadsheets may use different names for the same field. The transformation process therefore standardizes header names before applying dataset-specific rules.
    The mapper performs the following operations:
    1. Trims whitespace around each source header.
    2. Separates common camel-case boundaries, such as `ProductName` into `Product_Name`.
    3. Replaces punctuation, spaces, and other non-alphanumeric separators with underscores.
    4. Converts the normalized header to lowercase `snake_case`.
    5. Matches normalized headers against configured aliases.
    6. Uses compact matching to recognize equivalent headers whose differences are limited to spaces, underscores, or punctuation.
    7. Applies field-specific heuristics when an exact alias has not matched.
    8. Logs the mappings and any headers that remain unmapped.
    9. If multiple source columns map to the same canonical field, retains the first mapped column and logs a warning about the duplicate canonical header.
    Examples of header variants that can be mapped include:
    | Canonical field | Example source headers |
    |---|---|
    | `product_name` | `Product Name`, `ProductName`, `Item Description`, `Product Title`, `Name of Product` |
    | `unit_purchase_price` | `Purchase Price`, `Cost Price`, `Unit Cost`, `Buying Price` |
    | `estimated_price_php` | `Estimated Price (PHP)`, `Selling Price`, `Retail Price`, `Unit Price`, `SRP` |
    | `stocks` | `Stocks`, `Stock Quantity`, `Current Stock`, `On Hand`, `Available Quantity` |
    | `sales_date` | `Sales Date`, `Transaction Date`, `Date Sold`, `Order Date` |
    | `quantity` | `Quantity`, `Qty`, `Units Sold`, `Sold Quantity` |
    | `unit_sold` | `Sales Amount`, `Total Amount`, `Line Amount`, `Revenue` |
    | `analysis_date` | `Analysis Date`, `Created At`, `Timestamp` |
    | `paint_components` | `Paint Components`, `Components`, `Formulation`, `Mixture`, `Ingredients` |
    This list is illustrative rather than exhaustive. The aliases and heuristics configured in the transformation script determine which variants are recognized.
    ### Header mapping limitations
    Flexible mapping is the primary solution for variations in header naming, but it cannot infer every possible business meaning from an unfamiliar column. For example, a header such as `Amount` could refer to sales revenue, a purchase cost, a discount, or a payment. Ambiguous names should be added to the correct alias list only after their meaning is confirmed.
    When a required canonical field is not mapped, the script may add that field with a missing value to maintain the output schema. This can cause affected records to fail a later validation rule. Therefore, unmapped headers must be reviewed in the log rather than assuming that flexible mapping has recognized every field.
    ## 2.2 Data Profiling Procedure
    Before changing cleaning rules, profile each extracted dataset to identify:
    - Number of rows and columns.
    - Original column names and their mapped canonical names.
    - Unmapped or duplicate canonical headers.
    - Missing, null, empty, and whitespace-only values.
    - Numeric values that cannot be converted.
    - Negative prices, stock quantities, quantities, or sales amounts.
    - Invalid or ambiguous dates.
    - Product references that do not match the Product dataset.
    - Duplicate nonblank Paint Analysis identifiers.
    - Malformed RGB and paint-component values.
    The actual counts and percentages must be obtained by running the pipeline against the current source data. They should not be estimated or presented as measured results before the run is completed.
    ## 2.3 Profiling Results
    Use the following table to document actual findings after profiling:
    | Dataset | Issue | Number of affected records | Treatment |
    |---|---|---:|---|
    | Products | Unmapped or ambiguous headers | To be measured | Update aliases only after confirming field meaning. |
    | Products | Missing product name | To be measured | Quarantine under `C-02`. |
    | Products | Invalid price or stock value | To be measured | Quarantine under `C-03` when a nonblank value cannot be converted or violates a rule. |
    | Sales | Unmapped or ambiguous headers | To be measured | Update aliases only after confirming field meaning. |
    | Sales | Missing or unmatched product reference | To be measured | Quarantine under `C-02` or `C-05`, as applicable. |
    | Sales | Invalid quantity, amount, or date | To be measured | Quarantine under `C-03` or `C-06`, as applicable. |
    | Paint Analysis | Duplicate nonblank ID | To be measured | Quarantine subsequent duplicate IDs under `C-04`. |
    | Paint Analysis | Invalid RGB or component structure | To be measured | Quarantine under `C-07` or `C-08`. |
    | Paint Analysis | Invalid analysis date | To be measured | Quarantine under `C-06`. |
    ---
    # 3. Data Cleaning Rules
    ## 3.1 Cleaning Principles
    The cleaning process follows these principles:
    1. **Normalize names before validating values.** Flexible header mapping is applied before dataset-specific cleaning.
    2. **Preserve source meaning.** Cleaning must not change a value's intended meaning.
    3. **Trim text rather than rewriting it.** Leading and trailing whitespace is removed. Internal words, product sizes, model codes, and formulations are not changed automatically.
    4. **Treat missing values according to field requirements.** Missing optional values can remain missing. A missing required value is rejected only where the current validation rules require it.
    5. **Convert only interpretable values.** Values such as `₱1,250.00` can be converted to a decimal. A nonblank value such as `unknown` cannot be treated as a valid price without an approved business rule.
    6. **Do not silently replace invalid values with zero.** Zero is a meaningful value for prices, stock, quantities, and sales amounts and must not be used as a default for missing or invalid data.
    7. **Quarantine rather than silently discard rejected records.** Each rejected record must retain its original source values and the applicable rule ID and reason.
    8. **Do not confuse header issues with value issues.** A correctly mapped column can still contain invalid data, and an unfamiliar header can cause the expected field to be missing.
    ## 3.2 Cleaning Rules
    The following rules document the current behavior of `transformation.py`.
    | Rule ID | Rule name | Dataset and field | Condition | Current action |
    |---|---|---|---|---|
    | C-01 | Text trimming | Product: `product_name`, `category`, `brand`; Sales: `brand`, `product`, `category`, `season`; Paint Analysis: `id`, `color`, `rgb`, `paint_components` | A text value has leading or trailing whitespace. | Trim leading and trailing whitespace. The script tracks a record as modified when it detects a change. |
    | C-02 | Required identifying value | Products: `product_name`; Sales: `product` | Product name or Sales product reference is blank or missing. | Quarantine the record. |
    | C-03 | Numeric validation | Products: `unit_purchase_price`, `estimated_price_php`, `stocks`; Sales: `quantity`, `unit_sold` | A nonblank numeric value cannot be converted, or a value violates a nonnegative constraint. | Quarantine the record. Stock and quantity must be whole numbers. Negative prices, stocks, quantities, or sales amounts are rejected. |
    | C-04 | Duplicate Paint Analysis ID | Paint Analysis: `id` | A nonblank ID has already appeared in the same input dataset. | Quarantine the later record with the duplicate ID. Blank IDs are not rejected by this duplicate check. |
    | C-05 | Sales product-reference check | Sales: `product`; Products: `product_name` | A nonblank Sales product reference does not match any nonblank accepted Product name, when the accepted Product list is nonempty. | Quarantine the Sales record as unmatched. Matching is case-insensitive and trims leading/trailing spaces; it is not fuzzy matching. |
    | C-06 | Date validation | Sales: `sales_date`; Paint Analysis: `analysis_date` | A nonblank date cannot be parsed. | Quarantine the record. Valid Sales dates are output as date strings; valid analysis dates are output as ISO-formatted datetime strings. |
    | C-07 | RGB validation | Paint Analysis: `rgb` | A nonblank RGB value does not have an accepted three-channel structure or contains a channel outside 0–255. | Quarantine the record. Missing RGB values are allowed by this rule. |
    | C-08 | Paint-component JSON validation | Paint Analysis: `paint_components` | A string begins with `{` or `[` but is not valid JSON. | Quarantine the record. Blank values and ordinary text that does not begin with `{` or `[` pass this check. |
    ## 3.3 Missing Values and Optional Fields
    The current script does not require every field to be populated.
    ### Products
    - `product_name` is required. A blank value is rejected under `C-02`.
    - `unit_purchase_price` and `estimated_price_php` may be missing. If supplied as nonblank values, they must be convertible to decimals and must not be negative.
    - `stocks` may be missing. If supplied as a nonblank value, it must be a whole number and must not be negative.
    - `category` and `brand` may be missing.
    ### Sales
    - `product` is required. A blank value is rejected under `C-02`.
    - `quantity`, `unit_sold`, and `sales_date` may be missing under the current code. If supplied as nonblank values, they must pass their respective conversion and validation rules.
    - `brand`, `category`, and `season` may be missing.
    ### Paint Analysis
    - `id`, `color`, `rgb`, `paint_components`, and `analysis_date` may be missing under the current rules.
    - A nonblank duplicate `id` is rejected.
    - Nonblank RGB, JSON-like component values, and dates are checked according to `C-06`, `C-07`, and `C-08`.
    **Business-rule note:** If Garcia Paint Center requires additional fields to be mandatory, those requirements must be agreed upon and explicitly implemented. The documentation should not describe a field as mandatory when the current script allows it to be missing.
    ## 3.4 Numeric Conversion Rules
    The current numeric helper removes commas, the Philippine peso symbol (`₱`), and the dollar symbol (`$`) before attempting decimal conversion.
    Examples:
    | Input | Field | Expected behavior |
    |---|---|---|
    | `₱1,250.00` | Price or sales amount | Converts to `1250.00`. |
    | `1,250` | Stock or quantity | Converts to integer `1250`. |
    | `10.5` | Stock or quantity | Rejected because it is not a whole number. |
    | `-5` | Stock or quantity | Rejected because the value is negative. |
    | `N/A` | Numeric field | Rejected when nonblank because it cannot be converted. |
    | Blank value | Optional numeric field | Retained as missing. |
    These examples describe the current conversion logic. If the source contains other formats, such as parentheses for negative values, text-based units, or locale-specific decimal separators, those formats should be handled only after their meaning is confirmed.
    ## 3.5 Product-Name Matching Rules
    Sales records are checked against the Product dataset after Product transformation. The current comparison trims leading/trailing spaces and ignores capitalization.
    The current logic does **not** automatically equate:
    - Different product sizes, such as `Paint 1L` and `Paint 4L`.
    - Different formulations or finishes.
    - Abbreviations and full names that have not been configured.
    - Internal repeated spaces or punctuation differences.
    - Misspellings or similar but distinct names.
    If these variations occur in the actual data, the team should create a reviewed matching-normalization rule or a verified product crosswalk. Fuzzy matching must not automatically merge similar products without review because it can connect a sale to the wrong product.
    The current script applies `C-05` only if the accepted Product list is nonempty. If no valid Product names remain, this particular match check does not reject Sales records. The pipeline should therefore review Product output counts before approving Sales output.
    ## 3.6 Duplicate Handling
    The current script checks duplicate identifiers only for Paint Analysis records with nonblank `id` values. The later occurrence of a duplicate ID is quarantined.
    The script generates `product_id` and `sales_id` for Product and Sales outputs, so it does not use source IDs to detect duplicate Product or Sales transactions. Generated IDs identify output rows but do not prove that the original records are unique.
    If duplicate Product or Sales rows are a concern, a separate duplicate-detection rule must be defined using verified source transaction IDs or an approved combination of fields. Records must not be removed merely because their values look similar; two genuine sales can contain the same product, date, quantity, and amount.
    ## 3.7 Cleaning Procedure
    1. Load the extracted DataFrames.
    2. Preserve a copy of each original dataset for quarantine records.
    3. Normalize and map source headers using aliases and field-specific heuristics.
    4. Log the mappings, unmapped headers, and duplicate canonical headers.
    5. Add missing expected output columns so the standardized schema can be constructed.
    6. Generate new Product and Sales identifiers.
    7. Trim applicable text values.
    8. Convert and validate numeric fields.
    9. Validate required Product and Sales product-reference values.
    10. Check Sales product references against accepted Product names.
    11. Parse and validate date values.
    12. Validate Paint Analysis RGB and paint-component values.
    13. Quarantine records that fail the applicable validation rules.
    14. Save transformed datasets, quarantine records, and run metadata.
    15. Review record counts, rejection reasons, schema checks, and final transformation status.
    ## 3.8 Quarantine Output
    The quarantine file contains these fields:
    | Field | Purpose |
    |---|---|
    | `batch_id` | Identifies the transformation batch. |
    | `source_dataset` | Identifies `products`, `sales`, or `paint_analysis`. |
    | `source_row_number` | Records the source row position as calculated by the script. |
    | `original_record` | Stores the original row as JSON text for review. |
    | `rule_id` | Identifies the rule that caused rejection. |
    | `quarantine_reason` | Describes why the record was rejected. |
    | `quarantine_timestamp` | Records when the quarantine entry was created. |
    The source row number is calculated as the DataFrame index plus two, which assumes a header row and a zero-based row index. If an extractor changes or resets row indexes, this number may not equal the original file's physical row number.
    Quarantine entries should be grouped by `source_dataset`, `rule_id`, and `quarantine_reason` to identify recurring problems. Repeated rejection reasons should be investigated before changing validation rules.
    ---
    # 4. Standardization and Data Type Rules
    ## 4.1 Column Name Standardization
    All source headers are normalized to lowercase `snake_case` before alias matching. The resulting dataset uses the canonical output fields defined in the transformation script.
    The mapper uses exact normalized aliases first, compact-equivalent aliases second, and limited heuristics afterward. Unknown headers are logged and preserved during mapping but are not included in the final output unless they correspond to a defined output field.
    If two source columns map to the same canonical field, the current implementation keeps the first mapped column and drops the later duplicate canonical column. This behavior is logged. Such cases should be reviewed because the two columns may contain different information.
    ## 4.2 Output Schemas and Types
    | Output file | Output columns | Intended data representation |
    |---|---|---|
    | `products_transformed.csv` | `product_id`, `product_name`, `unit_purchase_price`, `estimated_price_php`, `stocks`, `category`, `brand` | Generated string ID; text fields; decimal prices; integer stock |
    | `sales_transformed.csv` | `sales_id`, `brand`, `product`, `category`, `sales_date`, `quantity`, `unit_sold`, `season` | Generated string ID; text fields; date string; integer quantity; decimal sales amount |
    | `paint_analysis_transformed.csv` | `id`, `color`, `rgb`, `paint_components`, `analysis_date` | Source ID and text/structured fields; ISO-formatted datetime string |
    CSV files do not preserve database-native data types. The table above describes the intended representation of values in the CSV outputs; the Load stage must apply the corresponding target database types.
    ## 4.3 Source-to-Canonical Field Mapping
    ### Products
    | Canonical output field | Example accepted aliases |
    |---|---|
    | `product_name` | `product_name`, `product name`, `productname`, `product`, `item`, `item name`, `product description`, `item description`, `description`, `product title`, `name of product` |
    | `unit_purchase_price` | `unit_purchase_price`, `unit purchase price`, `purchase price`, `cost price`, `unit cost`, `buying price`, `acquisition cost` |
    | `estimated_price_php` | `estimated_price_php`, `estimated price (php)`, `estimated price php`, `estimated_price`, `price`, `unit price`, `selling price`, `retail price`, `selling price php`, `price php`, `srp`, `retail price php` |
    | `stocks` | `stocks`, `stock`, `inventory`, `stock quantity`, `quantity in stock`, `current stock`, `available stock`, `on hand`, `qty on hand`, `available quantity` |
    | `category` | `category`, `product category`, `classification`, `product type` |
    | `brand` | `brand`, `manufacturer`, `make` |
    ### Sales
    | Canonical output field | Example accepted aliases |
    |---|---|
    | `brand` | `brand` |
    | `product` | `product`, `product_name`, `product name`, `item`, `item name`, `product description`, `item description`, `description`, `product title` |
    | `category` | `category`, `product category`, `classification`, `product type` |
    | `sales_date` | `sales_date`, `sales date`, `date`, `transaction date`, `date sold`, `order date`, `date of sale` |
    | `quantity` | `quantity`, `qty`, `units sold`, `sold quantity`, `number of units`, `quantity sold` |
    | `unit_sold` | `unit_sold`, `unit sold`, `sales amount`, `total amount`, `line amount`, `total price`, `revenue` |
    | `season` | `season`, `seasonality`, `weather season` |
    ### Paint Analysis
    | Canonical output field | Example accepted aliases |
    |---|---|
    | `id` | `id` |
    | `color` | `color`, `color_hex`, `color hex` |
    | `rgb` | `rgb` |
    | `paint_components` | `paint_components`, `paint components` |
    | `analysis_date` | `analysis_date`, `analysis date` |
    The code also includes field-specific heuristics for some additional variants. The tables above list configured aliases; they should be kept synchronized with the actual alias dictionaries in `transformation.py`.
    ## 4.4 Generated Identifiers
    The script discards any source `product_id` and `sales_id` columns before generating new IDs:
    - Product IDs use the `PROD-` prefix.
    - Sales IDs use the `SALE-` prefix.
    These IDs are generated for every input row before row validation. Only IDs for accepted rows appear in the transformed output. Since a new random ID is generated on each run, rerunning the same source data will not preserve the same Product or Sales identifiers.
    Paint Analysis IDs are not generated by the current script. The source `id` is retained when available, and duplicate nonblank IDs are checked.
    ## 4.5 Conversion Failure Handling
    A nonblank value that fails conversion is quarantined according to the applicable cleaning rule. A blank optional value can remain missing.
    If a required canonical field is absent because the source header was not mapped, the script may create the field as missing. For Products, this can lead to `C-02` for a missing `product_name`; for Sales, it can lead to `C-02` for a missing `product`. Other unmapped fields may remain null and will not necessarily cause rejection under the current rules.
    ---
    # 5. Business Transformation Rules
    ## 5.1 Product Output
    The Product transformation:
    1. Maps source columns to canonical names.
    2. Removes the source `product_id` column, if present.
    3. Adds missing expected output columns.
    4. Generates a new `product_id` for each source row.
    5. Trims applicable text values.
    6. Converts purchase and estimated selling prices to decimals when values are present.
    7. Converts stock values to integers when values are present.
    8. Rejects blank product names, invalid numeric values, and negative prices or stock.
    9. Produces the standardized Product output and records rejected rows in quarantine.
    ## 5.2 Sales Output
    The Sales transformation:
    1. Maps source columns to canonical names.
    2. Removes the source `sales_id` column, if present.
    3. Adds missing expected output columns.
    4. Generates a new `sales_id` for each source row.
    5. Trims applicable text values.
    6. Checks for a missing product reference.
    7. Checks the Sales product name against accepted Product names when the Product list is nonempty.
    8. Converts quantity and sales amount values when present.
    9. Parses Sales dates when present.
    10. Quarantines rows that fail applicable validation rules.
    The current Sales transformation does not calculate a new line total or derive a seasonal label. It preserves the mapped `unit_sold` and `season` values after cleaning and validation.
    ## 5.3 Paint Analysis Output
    The Paint Analysis transformation:
    1. Maps source columns to canonical names.
    2. Adds any missing expected output columns.
    3. Trims applicable text values.
    4. Checks duplicate nonblank source IDs.
    5. Validates nonblank RGB values.
    6. Checks JSON-like strings in `paint_components`.
    7. Parses nonblank analysis dates.
    8. Quarantines rows that fail applicable rules.
    The current code does not calculate paint-component quantities or alter component formulations. It validates and preserves the available component information.
    ## 5.4 Matching, Aggregation, and Filtering
    Sales-to-Product matching is based on normalized product names. The current code does not join Product columns into the Sales output, generate a foreign key, or perform fuzzy matching.
    The current script also does not aggregate sales. If aggregated sales are needed for forecasting or reporting, they should be defined as a separate, documented transformation with a specified grouping level (for example, by day, month, product, or category) and a clear aggregation formula.
    Paint Analysis remains a separate output because the current transformation does not establish a direct relationship between its records and Product or Sales records.
    ---
    # 6. Transformed Data Contract and Naming Convention
    ## 6.1 Required Output Schema
    | Output file | Required output columns |
    |---|---|
    | `products_transformed.csv` | `product_id`, `product_name`, `unit_purchase_price`, `estimated_price_php`, `stocks`, `category`, `brand` |
    | `sales_transformed.csv` | `sales_id`, `brand`, `product`, `category`, `sales_date`, `quantity`, `unit_sold`, `season` |
    | `paint_analysis_transformed.csv` | `id`, `color`, `rgb`, `paint_components`, `analysis_date` |
    The output columns follow the schemas defined in the script. Missing source columns are added as missing values before validation, so a matching output schema alone does not prove that source fields were correctly mapped or populated.
    ## 6.2 Naming Conventions
    | Item | Convention |
    |---|---|
    | Source header normalization | Lowercase `snake_case` |
    | Canonical fields | Use the defined output column names |
    | Generated Product identifier | `product_id`, with a `PROD-` prefix |
    | Generated Sales identifier | `sales_id`, with a `SALE-` prefix |
    | Paint Analysis identifier | Preserve source `id` when available |
    | Output files | Use the three documented transformed CSV filenames |
    | Unknown source columns | Log them; do not include them in the fixed output schema unless explicitly mapped |
    ## 6.3 Schema Consistency
    Before processing, source headers are normalized and mapped. The transformation then creates the expected output columns and orders them according to the documented schema.
    The current schema validation checks the output column names and order. It does not independently prove that every source field was mapped correctly. Therefore, the mapping log and missing-value checks must also be reviewed.
    ---
    # 7. Transformation Validation and Data Quality Checks
    ## 7.1 Validation Checks
    | Check | Purpose | Current criterion |
    |---|---|---|
    | Output schema | Confirm consistent output columns | Output column names and order match the documented schema. |
    | Record reconciliation | Account for every input row | `records_read = records_output + records_rejected` for each dataset. |
    | Generated IDs | Check generated Product and Sales IDs | Accepted output IDs are nonblank and unique within that run. |
    | Product reference check | Identify Sales records that cannot be matched | Apply the current exact normalized-name comparison when the accepted Product list is nonempty. |
    | Date validation | Detect unparseable nonblank dates | Invalid date values are quarantined. |
    | RGB validation | Check RGB structure and channel ranges | Nonblank values must meet the accepted RGB structure and use channel values from 0 to 255. |
    | Paint component validation | Detect malformed JSON-like strings | Strings beginning with `{` or `[` must be valid JSON. |
    | Rejection-rate review | Determine whether manual review is needed | Calculate the rate across all datasets and compare it with the configured threshold. |
    ## 7.2 Record Reconciliation
    The pipeline checks:
    `records_read = records_output + records_rejected`
    Modified records that pass validation remain part of `records_output`; `records_modified` is a separate metric and is not added to the reconciliation equation.
    This check verifies record accounting. It does not establish that accepted records are semantically correct, so the team must also inspect mapping logs, quarantine reasons, and dataset-specific metrics.
    ## 7.3 Rejection Rate
    The current code calculates the overall rejection rate across all three datasets:
    `Rejection Rate (%) = (Total records rejected / Total records read) × 100`
    The code uses a 1% threshold:
    - At or below 1%, the rejection-rate check passes if at least one record was read.
    - Above 1%, the batch is marked for manual review.
    - If no records were read, the transformation fails rather than being automatically approved.
    A high rejection rate should first be investigated by dataset, rule ID, and quarantine reason. The threshold is a review trigger, not a reason to weaken validation or accept invalid data. If a threshold is changed, the project team should document and approve the change.
    ## 7.4 Status Outcomes
    The script can report:
    - `SUCCESS`: no records were rejected and critical checks passed.
    - `SUCCESS_WITH_QUARANTINE`: some records were rejected, but the total rejection rate did not exceed the configured 1% threshold and critical checks passed.
    - `MANUAL_REVIEW`: the rejection rate exceeded 1%, while critical checks did not fail.
    - `FAILED`: a critical schema, record-reconciliation, or generated-ID check failed, no input records were read, or an exception interrupted processing.
    The status describes the transformation run. It does not replace manual investigation of repeated data-quality problems.
    ---
    # 8. Transformation Metadata and Logs
    ## 8.1 Metadata Fields
    The script records run information in its transformation log and prepares metadata with fields such as:
    | Field | Purpose |
    |---|---|
    | `run_id` | Unique identifier for the transformation run. |
    | `parent_run_id` | Extraction run reference, when supplied by the extractor. |
    | `batch_id` | Identifier for the transformation batch. |
    | `batch_window` | Description of the processed input range. |
    | `table_name` | Names of the datasets processed. |
    | `start_timestamp` | Transformation start time in UTC. |
    | `end_timestamp` | Transformation end time in UTC. |
    | `status` | Overall transformation status. |
    | `records_read` | Total records received across the datasets. |
    | `records_output` | Total accepted records written to output. |
    | `records_rejected` | Total quarantined records. |
    | `records_modified` | Number of retained records marked as modified. |
    | `dataset_metrics` | Per-dataset read, output, modified, and rejected counts. |
    | `rules_applied` | Rule IDs associated with each dataset. |
    | `validation_results` | Results of schema, reconciliation, generated-ID, and rejection-rate checks. |
    | `error_message` | Error information, if any. |
    | `duration_seconds` | Elapsed processing time. |
    | `code_version` / `rules_version` | Transformation and rule version identifiers. |
    | `transformation_log` | Path to the transformation log. |
    | `quarantine_file` | Path to the quarantine output. |
    The current extraction integration returns a placeholder parent run ID rather than a real extraction run ID. This should be updated if the extraction stage exposes run metadata.
    ## 8.2 Log Contents
    The log records:
    - Run and batch identifiers.
    - Start and finish timestamps.
    - Extracted record counts.
    - Header mappings and unmapped headers.
    - Dataset-level read, output, modified, and rejected counts.
    - Rules associated with each dataset.
    - Validation results and overall rejection rate.
    - Quarantine file location and count.
    - Exceptions and error details.
    The log should be reviewed together with `quarantine.csv`. The log provides a summary; the quarantine file provides record-level reasons for rejection.
    ---
    # 9. Log Retention, Quarantined Data, and Access
    ## 9.1 Retention
    The current script defines a 30-day log-retention policy. The constant documents the intended retention period; automatic deletion or archival is not implemented by the transformation script itself.
    | Item | Specification |
    |---|---|
    | Log location | `logs/transformation.log` |
    | Quarantine location | `transformation/output/quarantine.csv` |
    | Intended retention | 30 days, subject to project policy |
    | Access | Authorized team members responsible for the ETL process |
    | Deletion or archival | Managed separately; do not assume the script automatically deletes old records |
    ## 9.2 Data Preservation
    The transformation functions keep a copy of each original input DataFrame for the purpose of recording rejected records. The extracted source files are not intentionally overwritten by the transformation process.
    Quarantine records should be reviewed and retained according to the project's data-handling requirements. If logs or quarantine records contain business-sensitive information, access should be limited to authorized team members.
    ---
    # 10. Acceptance and Handover Rules
    ## 10.1 Acceptance Criteria
    The output is considered ready for handover only after reviewing the transformation status, output schema, record reconciliation, generated IDs, rejection rate, and quarantine reasons.
    | Rule | Condition | Action |
    |---|---|---|
    | A-01: Successful handover | Critical checks pass and rejection rate is at or below 1%, with no rejected records | Mark as `SUCCESS` and prepare outputs for handover. |
    | A-02: Handover with quarantine | Critical checks pass and some records are rejected, but rejection rate is at or below 1% | Mark as `SUCCESS_WITH_QUARANTINE`; retain quarantine for review. |
    | A-03: Manual review | Rejection rate exceeds 1% without a critical validation failure | Hold for review and investigate the dataset and rules causing rejection. |
    | A-04: Failed transformation | Critical validation fails, no input records are read, or an exception interrupts processing | Do not approve the batch; investigate and rerun after correcting the issue. |
    These criteria describe the current script's behavior. The project team should approve the 1% threshold and determine whether any quarantined records require resolution before the Load stage.
    ## 10.2 Handover Package
    The handover package includes:
    - `products_transformed.csv`
    - `sales_transformed.csv`
    - `paint_analysis_transformed.csv`
    - `quarantine.csv`
    - Transformation logs and validation results.
    - Dataset-level record counts and rejection reasons.
    - Run and batch identifiers.
    - Overall transformation status.
    The transformation stage ends when the outputs and supporting evidence are ready for the Load stage. Loading into the target database or warehouse is outside the scope of this stage.
    ---
## 11. Common Transformation Problems and Mitigation
During the transformation stage, several data quality problems may occur when cleaning, standardizing, validating, and matching the extracted Product, Sales, and Paint Analysis datasets. These problems can affect the number of accepted records and the reliability of the transformed outputs. The following table identifies potential problems and the corresponding mitigation strategies.
| Problem | Possible Cause | Impact | Mitigation |
|---|---|---|---|
| Unrecognized Column Headers | Source CSV files use different column names, abbreviations, capitalization, or spacing than the expected output schema. | Required fields may remain empty, causing records to fail validation or produce incomplete transformed data. | Use flexible header mapping with predefined aliases and header normalization. Review unmapped headers in the transformation log and add verified aliases when necessary. |
| Invalid or Missing Price Values | Purchase prices or estimated selling prices contain nonnumeric text, currency symbols, commas, or missing values. | Invalid prices may cause product records to be quarantined or reduce the reliability of product pricing analysis. | Remove supported currency symbols and thousands separators before numeric conversion. Preserve missing optional prices and quarantine nonblank values that cannot be converted safely. |
| Inconsistent Stock Values | Stock data may contain whole numbers, decimal values, text, or descriptions such as `N/A` and `Out of stock`. | Strict integer validation may reject otherwise usable product records or make stock values difficult to interpret consistently. | Preserve stock values without requiring every value to be an integer. Keep the original meaning of text values and avoid automatically converting unknown values to zero. Apply numeric conversion only when the value can be interpreted safely. |
| Unmatched Sales Product Names | Sales records may contain product codes, differences in capitalization, extra spaces, punctuation, or packaging abbreviations that differ from the Product catalog. | Valid sales records may be quarantined because the transformation cannot identify their corresponding products. | Normalize capitalization, whitespace, punctuation, recognized product-code prefixes, and approved packaging abbreviations before matching. Review remaining unmatched names and use a verified product crosswalk when necessary. |
| Invalid Sales Dates | Sales dates may use inconsistent formats, contain invalid dates, or include values that cannot be interpreted unambiguously. | Sales records may be quarantined, affecting sales reporting and date-based analysis. | Parse supported date formats and standardize valid dates into a consistent output format. Quarantine nonblank dates that cannot be interpreted safely and review ambiguous formats before conversion. |
| Invalid RGB Values | Paint Analysis records contain RGB values with missing channels, malformed structures, or channel values outside the range of 0–255. | Invalid color information may affect paint color analysis and the reliability of stored color records. | Validate the RGB structure and ensure each channel falls within the accepted range. Quarantine invalid nonblank values while allowing missing RGB values when permitted by the validation rules. |
| Malformed Paint Components | The `paint_components` field contains malformed JSON when the value is intended to be a JSON object or array. | Paint component information may not be usable by later processes that expect structured data. | Validate JSON-formatted component values before accepting them. Quarantine malformed JSON-like values and preserve the original record for correction. |
| Duplicate Paint Analysis IDs | Multiple Paint Analysis records contain the same nonblank source identifier. | Duplicate identifiers may create ambiguity when identifying or processing paint analysis records. | Check nonblank source IDs for duplicates within the input dataset. Retain the first occurrence and quarantine subsequent duplicates for review. |
| High Quarantine or Rejection Rate | Multiple records fail validation because of incorrect values, unmapped headers, or unmatched product references. | Too many records may be excluded from transformed outputs, reducing data completeness and delaying the next ETL stage. | Group quarantine records by dataset, rule ID, and rejection reason. Correct source data or improve verified mapping and normalization rules. Recalculate the rejection rate and require manual review when it exceeds the configured 1% threshold. |
| Inconsistent Generated Identifiers | Product and Sales identifiers are regenerated whenever the transformation script runs. | Identifiers may change between runs, making it difficult to compare records across batches or maintain stable references. | Document generated IDs as run-specific identifiers. If stable identifiers are required across runs, use persistent source identifiers or an approved, reproducible key-generation strategy. |
These mitigation strategies help reduce avoidable record rejections while preserving data quality. Validation rules should only be adjusted when the revised behavior is consistent with the source data and approved business requirements. Records that cannot be interpreted safely should remain available in `quarantine.csv` for investigation rather than being silently discarded or incorrectly transformed.

# Expected Output
    Stage 2 produces standardized and validated Product, Sales, and Paint Analysis CSV files, a quarantine file for rejected records, and logs describing the transformation run.
    Flexible header mapping handles configured naming variations before cleaning and validation. The remaining rules address the quality and consistency of the values themselves. This separation allows the pipeline to accommodate different source-header conventions without silently accepting invalid prices, quantities, dates, RGB values, component data, or unmatched product references.