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

## Data Set Column Mapping

When importing a data set, you must pass in a column mapping object to map the column header strings to variable Ids.

Expected Column Mapping Format:

    {
      'Header Name': _variableId_
      ...
    }

## Data Set "Schema"

The importer will automatically determine the "Schema" of a data set and save it to the data set
entity upon successful import. The schema captures which variables/attributes are 
represented in the data set. Variables are mapped to columns using the "Data Set Column Mapping",
while attributes are automatically mapped using the attribute's 'key' property. If there are no 
attribute matches found in the system, an error will be thrown.

Schema Format:

    [
        {
            variable: _variable_id_,
            // If Categorical
            attributes: [
                _attribute_id_,
                ...
            ]
        },
        ...
    ]


## Import File

### Expectations

* The target data set ID will be provided
* The column mapping will be provided
* All variables and attributes are pre-created in the system (if matches aren't found, an error is thrown)

### Import steps

1. Ingest all data rows into facts table
    a. Map column headers to variables using the mapping provided
    b. Match attributes keys to attributes in the system for the column's variable
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

    [
        {
            key: _column_header_,
            match: true | false,
            variable: _variable_id_,
            attributes: [                      // Only if categorical
                {
                    key: _attribute_name_,
                    match: true | false,
                    attribute: _attribute_id_
                },
                ...              
            ]
        },
        ...
    ]

### Importer

* Import(dataSetId, mappings, filePath [, batchSize])
    * Returns results

Results format

  TBD (Job)
