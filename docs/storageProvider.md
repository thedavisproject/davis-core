Storage Provider
====================

A Davis instance must be configured to use a storage provider. A storage provider is the primary means for persisting data, and will generally utilize some sort of database. Storage providers must expose the following top level API to seamlessly interoperate with the rest of the system.

## Structure

The top level API must follow this structure. See below for details on each leaf.

    {
        entities: {
            query,
            create,
            update,
            delete
        },
        data: {
            query,
            create,
            delete
        },
        publish: {
            publishEntities,
            publishFacts
        },
        transact
    }


## Methods

### Entities

#### Query

Query storage for any entities that match the specified expression.

    entities.query(catalog, entityType[, expression, options])

* **catalog**
    * *String*
    * The catalog environment (i.e. master, web, etc)
* **entityType**
    * *String*
    * The type of entity to query
* **expression** *(optional)*
    * *Array*
    * See entityQuery.md documentation in davis-model
    * If no properties are specified, no filters are applied (e.g. all are returned)
* **options** *(optional)*
    * *Object*
    * See entityQuery.md documentation in davis-model
* **Returns**
    * *Task [Object]*
    * An array of entity objects that match the query, wrapped in a Task.

#### Create

Creates the specified entities in storage.

    entities.create(catalog, entities)

* **catalog**
    * *String*
    * The catalog environment (i.e. master, web, etc)
* **entities**
    * *[Object]*
    * The entities to create
    * Each entity's `id` property must be undefined or null. A unique ID must be assigned to the entity upon creation.
* **Returns**
    * *Task [Object]*
    * An array of the created entities (with assigned IDs), wrapped in a Task.

#### Update

Updates the specified entities in storage.

    entities.update(catalog, entities)

* **catalog**
    * *String*
    * The catalog environment (i.e. master, web, etc)
* **entities**
    * *[Object]*
    * The entities to update
    * Each entity's `id` property must be defined and non-null
* **Returns**
    * *Task [Object]*
    * An array of the updated entities, wrapped in a Task.

#### Delete

Deletes the specified entities.

    entities.delete(catalog, entityType, ids)

* **catalog**
    * *String*
    * The catalog environment (i.e. master, web, etc)
* **entityType**
    * *String*
    * The type of entity to delete
* **ids**
    * *[Number]*
    * The entity IDs
* **Returns**
    * *Task bool*
    * Success/error, wrapped in a Task


### Data

#### Query

Queries data in storage (non-entity).

    data.query(catalog, filters[, dataSetIds])

* **catalog**
    * *String*
    * The catalog environment (i.e. master, web, etc)
* **filters**
    * *Array*
    * The filters to apply to the query. [See query documentation](./query.md)
* **dataSetIds** (Optional)
    * *Array*
    * Data sets to query
    * If omitted, all data sets will be queried
* **Returns**
    * *Task [Object]*
    * Array of "Individual" objects, wrapped in a Task

#### Create

Creates data in storage (non-entity). 

    data.create(catalog, individuals)

* **catalog**
    * *String*
    * The catalog environment (i.e. master, web, etc)
* **individuals**
    * *[Object]*
    * The *individuals* to create.
* **Returns**
    * *Task Number*
    * Count of individuals processed, wrapped in a Task

#### Delete

Deletes data from storage (non-entity) for the given data set.

    data.delete(catalog, dataSetId)

* **catalog**
    * *String*
    * The catalog environment (i.e. master, web, etc)
* **dataSetId**
    * *Number*
    * The ID of the data set to target
* **Returns**
    * *Task*
    
### Action Log

Read and write entries to the action log.

##### Query

Query action log entries.

    log.query(catalog[, properties[, dateRange]])

* **catalog**
    * *String*
    * The catalog environment (i.e. master, web, etc)
* **properties** *(optional)*
    * *Object*
    * The properties to filter on. Only properties with primitive values (String, Number, etc) may be specified. Values are supplied as an array of possible matches.
    * If no properties are specified, no filters are applied (e.g. all are returned)
    * e.g. `{ subjectType: ['dataset'], subjectId: [1] }`
* **dateRange** *(optional)*
    * *[Date, Date]*
    * The start and end dates
* **Returns**
    * *Task [Object]*
    * An array of action log entry objects that match the query, wrapped in a Task.

##### Create

Create an action log entry.

    log.create(catalog, logEntries)

* **catalog**
    * *String*
    * The catalog environment (i.e. master, web, etc)
* **logEntries**
    * *[Object]*
    * The log entries to create
    * Each entry's `id` property must be undefined or null. A unique ID must be assigned to the entity upon creation.
* **Returns**
    * *Task [Object]*
    * An array of the created log entries (with assigned IDs), wrapped in a Task.

### Publish

#### Publish Entities

    publishEntities(sourceCatalog, targetCatalog, [entityTypes])

* **sourceCatalog**
    * *String*
    * The catalog environment to publish *from* (i.e. master, web, etc)
* **targetCatalog**
    * *String*
    * The catalog environment to publish *to* (i.e. master, web, etc)
* **entityTypes**
    * *[String]* 
    * The entity types that should be fully published
* **Returns**
    * *Task Bool 

#### Publish Data

    publishData(sourceCatalog, targetCatalog, [dataSetIds])

* **sourceCatalog**
    * *String*
    * The catalog environment to publish *from* (i.e. master, web, etc)
* **targetCatalog**
    * *String*
    * The catalog environment to publish *to* (i.e. master, web, etc)
* **dataSetIds**
    * *[Number]* 
    * The dataSetIds whose data should be published
* **Returns**
    * *Task Bool 

### Transact

Provides a context for executing multiple storage actions within a single "transaction". Transact takes a function that will be immediately invoked with 3 arguments:

    * A slighly modified version of the full Storage Api 
      * This nested `transact` method should tack any storage operations onto the top level transaction, effectively flattening all operations to a single transaction
    * A *commit* function
    * A *rollback* function

Any standard methods may be called on the *storageApi* object, followed by calling either the "rollback" or "commit" functions to end the transaction.

    transact(transactionScopeFn[, timeout])

* **transactionScopeFn**
    * *Function* that has parameters: (storageApi, commit, rollback)
        * **storageApi**
            * *Object*
            * Is identical to the full top level Storage API, minus the "transact" method
        * **commit**
            * *Function* that takes 0 parameters
            * This should be called to commit the transaction
        * **rollback**
            * *Function* that takes 0 parameters
            * This should be called to roll back the transaction
        * ** Returns **
            * Task (optional). If this function returns a Task, the Task must be evaluated and commit/rollback automatically called for resolved/rejected cases
* **timeout**
  * *Number* (Optional)
  * Sets the timeout before the transaction is rolled back and rejected
* **Returns**
  * **Task**
