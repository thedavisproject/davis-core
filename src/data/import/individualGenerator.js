const R = require('ramda');
const util = require('davis-shared');
const { thread } = util.fp;
const { isNilOrEmpty } = util.string;
const Task = require('data.task');
const Either = require('data.either');
const { variable, attribute, individual, fact} = require('davis-model');
const Transform = require('stream').Transform;

module.exports =
  ({
    entityRepository
  }) =>
  {
    const rowError = (rowIndex, error) => {
      return `Error: Row ${rowIndex}. ` + error;
    };

    const cleanNumericalValue = value => {
      return value.toString().replace(/[$,%]/gi,'');
    };

    const createFact = R.curry((rowIndex, mapping, key, value) => {
      // Ignore the value if there is no mapping. this means the column is ignored and not imported
      if(!mapping){
        return null;
      }
      if(!mapping.variable){
        return Either.Left(rowError(rowIndex, `Invalid mapping for column: ${key}`));
      }
      if(mapping.variable.type === variable.types.categorical){
        if(R.isNil(value)){
          return Either.Right(
            fact.newCategorical(mapping.variable.id, null));
        }

        var attr;

        if(isNilOrEmpty(value)){
          attr = null;
        }
        else{

          if(!mapping.attributes || !mapping.attributes[value]){
            return Either.Left(rowError(rowIndex, `Error: Row ${rowIndex}. Invalid mapping for attribute: ${mapping.variable.key}: ${value}`));
          }

          attr = mapping.attributes[value].id;
        }
        return Either.Right(
          fact.newCategorical(mapping.variable.id, attr));
      }
      else if(mapping.variable.type === variable.types.numerical){
        if(R.isNil(value)){
          return Either.Right(
            fact.newNumerical(mapping.variable.id, null));
        }

        const cleanValue = cleanNumericalValue(value);
        if(isNaN(cleanValue)){
          return Either.Left(rowError(rowIndex, `Error: Row ${rowIndex}. Non-numerical value for numerical variable: ${mapping.variable.key}: ${cleanValue}`));
        }
        return Either.Right(
          fact.newNumerical(mapping.variable.id, cleanValue));
      }
      else {
        if(R.isNil(value)){
          return Either.Right(
            fact.newText(mapping.variable.id, null));
        }

        return Either.Right(
          fact.newText(mapping.variable.id, value));
      }
    });

    const createIndividual = R.curry((dataSetId, mappings, rowIndex, rowValues) => {
      return thread(
        rowValues,
        R.toPairs,
        R.map(([key, value]) => createFact(rowIndex, mappings[key], key, value)),
        R.reject(R.isNil),
        R.sequence(Either.of),
        R.map(facts => individual.new(rowIndex, dataSetId, facts)));
    });

    const mergeEntitiesAndSchema = (schema, vars, attrs) => {

      const variableLens = R.lensProp('variable'),
        attributesLens = R.lensProp('attributes');

      const replaceWithVariableObj = R.over(variableLens, id => vars[id]);

      const replaceWithAttributesObjs = m => {
        if(!R.has('attributes', m)){
          return m;
        }

        return R.over(attributesLens,
          R.pipe(
            R.map(id => attrs[id]),
            R.indexBy(R.prop('key'))),
          m);
      };

      return thread(
        schema,
        R.map(replaceWithVariableObj),
        R.map(replaceWithAttributesObjs),
        R.indexBy(R.path(['variable', 'key'])));
    };

    const resolveEntityMappings = schema => {

      if(!schema){
        return Task.rejected('Invalid Schema. The Schema must be provided when importing data.');
      }

      const vars = thread(
        schema,
        R.map(R.prop('variable')),
        ids => entityRepository.queryById(variable.entityType, ids),
        R.map(R.indexBy(R.prop('id'))));

      const attrs = thread(
        schema,
        R.filter(R.has('attributes')),
        R.map(m => m.attributes),
        R.flatten,
        ids => entityRepository.queryById(attribute.entityType, ids),
        R.map(R.indexBy(R.prop('id'))));

      return thread(
        R.sequence(Task.of, [ vars, attrs ]),
        R.map(([v, a]) => mergeEntitiesAndSchema(schema, v, a)));
    };

    return {
      rawToIndividuals: (dataSetId, schema) => {

        const mappingsTask = resolveEntityMappings(schema);

        return mappingsTask.map(mappings => {

          var rowIndex = 0;

          const processDataRow = (reject, resolve, row) => {
            rowIndex += 1;

            const individualItem = createIndividual(
              dataSetId,
              mappings,
              rowIndex,
              row);

            // If error processing row
            individualItem.map(ind => {
              resolve(ind);
            }).orElse(error => {
              reject(error);
            });
          };

          return new Transform({
            objectMode: true,
            transform: (chunk, encoding, callback) => {
              function reject(error) {
                callback(new Error(error), null);
              }
              function resolve(ind) {
                callback(null, ind);
              }
              processDataRow(reject, resolve, chunk);
            }
          });
        });
      }
    };
  };
