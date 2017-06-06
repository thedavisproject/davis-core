Thoughts on Structuring Data
============================

* Imports will be structured with the following rules
    * 1 record (individual) per row
    * 1 variable per column

* Variables can be either quantitative or categorical (qualitative)

* Differences from Datacenter 1.x:
    * There is no single "Data" column
    * There are no mandatory variables (Location, TimeFrame, Datatype)

* Querying data will be very similar to SQL queries
    * Filter on categorical variables using exact matches (i.e. State = 'MA')
    * Filter on quantitiative variables using <,>,<=,>=,= (i.e. Percent <= .5)
    * Pare down result set in the query (i.e. only return the Percent,State,Year values)
    * Eventually allow aggregating quantitative variables
        * i.e. Get all States, Males, 2012, calculate the median Percent

Example 1:

| State | Year | Gender | Percent | Count    | 
|-------|------|--------|---------|----------| 
| MA    | 2012 | Male   | 0.5     | 10000000 | 
| MA    | 2012 | Female | 0.5     | 11000000 |

In this example, there are 3 categorical variables:

* State
* Year
* Gender

And 2 quantitative variables:

* Percent
* Count

Example 2:

| Year | Make  | Model  | Engine Displacement | MPG | 
|------|-------|--------|---------------------|-----| 
| 2016 | Honda | Civic  | 1.5                 | 35  | 
| 2016 | Honda | Accord | 2.4                 | 31  | 

In this example, there are 3 categorical variables:

* Year
* Make
* Model

And 2 quantitative variables:

* Engine Displacement
* MPG


## 1/23/2017 Thoughts on reclassifying variables

Requirements:

* Some variables must have "attributes" that are represented by entities in the system. Often times, attributes are special and need to be cross referenced between data sets, or even have their own web page (e.g. location profiles). These are currently known as "Categorical" variables.
    * Example: Variable = Location, Attribute = Massachusetts

* Some variables must have continuous, quantitative values. These are represented by long floating point values in the database.
    * Example: Variable = Dollars Spent, Value = 56345.57

* Some variables must have non-categorical, non-quantitative values.
    * Example: Variable = Youtube ID, Value = CNW9txOMYAg


Types of variables that are relevant to our purposes:

* Quantitative
    * Types
        * Interval - Quantitative where the difference between each value has a standard and equal meaning (i.e. Degrees Celcius)
        * Ratio - Same as interval, but zero is meaningful as the absence of the thing measured. (e.g. Degrees Kelvin, Percent of anything, age, etc).
    * Can be continuous or discrete
    * Also known as "measurement" variable?

* Qualitative
    * Categorical/Nominal is generally Qualitative. Represents a set of categories that data can be put into.
    * Ordinal - Categorical, but there is a clear order. (e.g. likert scales)

    
* Categorical
* Numerical (Quantitative)
* Textual (Qualitative, but non-categorical)
