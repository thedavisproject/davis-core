Glossary (Ubiquitous Language)
==============================

**Catalog** - An instance of data storage. Data can be published from one catalog to another. Typically, a front end targets a single catalog (web) while the admin targets a single catalog (master). Data is published from master to web.

**Davis** - The canonical name for this project. Davis is a data management and visualization system.

**Folder** - An organizational entity that can be used to organize Sheets. Other entities may be organizable by folders in the future.

**DataSet** - A logical group of related data, such as "Population by state", or "Automobile Fuel economy".

**Variable** - An overall grouping for a set of attributes or values that can be used to describe a single data row. For example, a variable may be "Location", "Weight", or "Percent". Variables can be either quantitative or categorical. Categorical variables express Attributes (see below), quantitative variables express numerical values.

**Attribute** - A member of a categorical variable. For example, the "Location" variable may have an attribute "Massachusetts".

**Individual** - An object described by a set of data (facts). Or, a single set of related variable attributes or values. For example, a Sheet that has variables of "Location", "Gender", and "Percent of Population" may have an individual:
    "Massachusetts", "Female", "0.55".

**Fact** - A single attribute or value that makes up an Individual. In the Individual example above, there are 3 facts: "Location = Massachusetts", "Gender = Female", "Percent of Population = 0.55".
