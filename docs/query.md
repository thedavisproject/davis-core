Query Interfaces
================

The Query interface will be the primary method for building data reports and querying data.

## Filters

The filters object will enumerate the categorical attributes, numerical/text values that should be matched to the resulting individuals.

    [
        {
            variable: _variableId_,
            type: categorical (or 0),
            attributes: [_attributeIds_]
        },
        {
            variable: _variableId_,
            type: numerical (or 1),
            value: _decimalValue_,
            comparator: <, <=, =, >=, >
        },
        {
            variable: _variableId_,
            type: text (or 2),
            value: _textValue_
        },
        ...
    ]

### Results

The results object will contain:

* The resulting individuals (data) that match the query
* The subset of variables/attributes that are represented in the query set
    * This subset may be used for cross referencing variable/attribute properties as well as identifying which attributes are available for further filtering.
* All results will be grouped by data set

Results structure:

    [
        {
            id: _dataSetId_,
            name: _dataSetName_,
            data: [
                [
                    {
                        variable: _variableId_,
                        attribute: _attributeId_  // For categorical
                    },
                    {
                        variable: _variableId_,
                        value: _decimalValue_     // For numerical
                    },
                    {
                        variable: _variableId_,
                        value: _textValue_        // For text
                    }
                    ...
                    ...
                ],
                ...
            ]
        }
    ]

## Methods

* Query(filters, dataSetId || [dataSetIds])
    * dataSetId/dataSetIds is optional
    * Returns Results object

* VariablesByDataSet(dataSetId || [dataSetIds])
    * Returns 'variables' portion of results object

## REST Api

The API query method is a GET request, so the filter object must be serialized into a query string as follows:

* Keys: ('c' || 'n' || 't') + _variable_id_
    * 'c' for categorical, 'n' for numerical, 't' for text
    * e.g. c45, n500, t75
* Values:
    * Categorical: comma separated list of ids. (e.g.  4,5,6,10)
    * Numerical: _operator_ + value (e.g. <10.5  or >=6)
    * Text: text value
