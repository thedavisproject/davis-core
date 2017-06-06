Repository Interfaces
====================

A repository will be the primary interface for CRUD operations on entities. It will have a generic interface that can be implemented by any provider.

## Methods

### General Entity

* ReadAll(entityType)
    * Returns: [entity]

* ReadById(entityType, id || [id])
    * Returns: [entity] -- always returns an array

* ReadByName(entityType, name || [name])
    * Returns: [entity] -- name may match more than one entity

* ReadByProperties(entityType, properties)
    * properties is an object that contains all props that should match
    * Returns: [entity]

* Create(entity || [entity])
    * Returns: [entity] -- created entities

* Update(entity || [entity])
    * Returns: [entity] -- updated entities

* Delete(entityType, id || [id])
    * Returns: bool -- (success/error)

### Hierarchy

* GetParent(entity)
    * Returns: Maybe entity

* GetChildren(entity)
    * Returns: [entity]
