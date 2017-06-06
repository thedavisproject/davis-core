File Import Process and Specs
=============================

## Analyze File

### Expectations

* Import file will be provided in CSV format (Excel in the future)
* Import file will be provided with the proper formatting
    * How to determine this?
* The target data set ID will be provided (data set entity should be created before importing data)

### Analyze Steps

1. Pull column header names
2. Attempt to match existing variables by name
    a. Match data set specific variables first
    b. Then try to match global variables
3. Determine unique values for categorical variables
4. Attempt to match existing attributes if existing variable is found
5. Return data sheet summary
    a. Includes: 
        i. Each column header, and whether it matched an existing variable (global or local). Also includes the detected variable type.
        ii. The set of attributes for categorical variables, and whether or not they matched existing variables by name.

### Summary Format

## Data Set "Schema"

Each data set must have a "Schema" configured before importing data. The schema is
what configures which variables/attributes are represented in the data set. In
order for the importer to understand how to import each row in the CSV file, the
schema must be first set on the indicator item.

Expected Schema Format:

    [
        {
            variable: _variable_id_,
            attributes: [             // If Categorical
                _attribute_id_,
                ...
            ]
        },
        ...
    ]


## Import File

### Expectations

* The target data set ID will be provided
* Schema must be pre-configured on the target data set

### Import steps

1. Ingest all data rows into facts table
    a. Match column headers to variables in the schema
    b. Match attributes names to attributes in the schema
    c. All a single transaction
    d. Should be batched for better performance 
    e. Will roll back transaction if any rows fail
2. Return results or error
    a. Number of rows inserted
    b. Row number and error for any errors

## Interfaces

### File Parser

* Parse(filePath)
    * Returns: Stream of results (1 row at a time)

Format for Row Output:

    {
        _column_name_: _value_,
        ...
    }

### Analyzer

* Analyze(dataSetId, dataStream)
    ** Returns data sheet summary

Summary format

    {
        _column_header_: {
            name: _column_header_,
            match: true | false,
            variable: _variable_id_,
            scope: local | global,
            type: categorical | quantitative,
            attributes: {                      // Only if categorical
                _attribute_name_: {
                    name: _attribute_name_,
                    match: true | false,
                    attribute: _attribute_id_
                },
                ...              
            }
        },
        ...
    }

### Importer

* Import(dataSetId, mappings, dataStream)
    * Returns results

Results format

    {
        status: 'Success' | 'Failure',
        dataSet: _data_set_id,
        rowsImported: _num_imported_,
        failures: [
            {
                row: _row_num_,
                error: _error_message_
            },
            ...
        ]
    }
