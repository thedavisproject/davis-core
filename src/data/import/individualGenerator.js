const R = require('ramda');
const util = require('davis-shared');
const { thread } = util.fp;
const { isNilOrEmpty } = util.string;
const Task = require('data.task');
const { variable, attribute, individual, fact, query: q} = require('davis-model');
const Transform = require('stream').Transform;

module.exports =
  ({
    entityRepository
  }) =>
  {
    return {
      rawToIndividuals: (dataSetId, columnMappings, {
        createMissingAttributes = false
      } = {}) => {

        const rowError = (rowIndex, error) => {
          return `Error: Row ${rowIndex}. ` + error;
        };

        const cleanNumericalValue = value => {
          return value.toString().replace(/[$,%]/gi,'');
        };

        const createFact = (rowIndex, mapping, key, value) => {
          // Ignore the value if there is no mapping. this means the column is ignored and not imported
          if(!mapping){
            return null;
          }
          if(!mapping.variable){
            return  Task.rejected(rowError(rowIndex, `Invalid mapping for column: ${key}`));
          }
          if(mapping.variable.type === variable.types.categorical){
            if(R.isNil(value)){
              return Task.of(
                fact.newCategorical(mapping.variable.id, null));
            }

            var attr;

            if(isNilOrEmpty(value)){
              attr = null;
            }
            else{

              if(!mapping.attributes || !mapping.attributes[value]){

                if(createMissingAttributes){
                  return entityRepository.create([attribute.new(null, value, mapping.variable.id)])
                    .chain(newAttributes => 
                      Task.of(fact.newCategorical(mapping.variable.id, newAttributes[0].id)));
                }
                else {
                  return Task.rejected(
                    rowError(
                      rowIndex,
                      `Error: Row ${rowIndex}. Attribute with key {${value}} does not exist for variable {${mapping.variable.name}, ${mapping.variable.id}}`));
                }
              }

              attr = mapping.attributes[value].id;
            }
            return Task.of(
              fact.newCategorical(mapping.variable.id, attr));
          }
          else if(mapping.variable.type === variable.types.numerical){
            if(R.isNil(value)){
              return Task.of(
                fact.newNumerical(mapping.variable.id, null));
            }

            const cleanValue = cleanNumericalValue(value);
            if(isNaN(cleanValue)){
              return Task.rejected(rowError(rowIndex, `Error: Row ${rowIndex}. Non-numerical value for numerical variable: ${mapping.variable.key}: ${cleanValue}`));
            }
            return Task.of(
              fact.newNumerical(mapping.variable.id, cleanValue));
          }
          else {
            if(R.isNil(value)){
              return Task.of(
                fact.newText(mapping.variable.id, null));
            }

            return Task.of(
              fact.newText(mapping.variable.id, value));
          }
        };

        const createIndividual = (dataSetId, mappings, rowIndex, rowValues) => thread(
          rowValues,
          R.toPairs,
          R.map(([key, value]) => createFact(rowIndex, mappings[key], key, value)),
          R.reject(R.isNil),
          R.sequence(Task.of),
          R.map(facts => individual.new(rowIndex, dataSetId, facts)));

        const mergeEntitiesAndMappings = (columnMappings, vars, attrs) => {

          const indexedVars = R.indexBy(R.prop('id'), vars);
          const groupedAttrs = R.groupBy(R.prop('variable'), attrs);

          function buildVariableMapping(varId){
            let mapping = {
              variable: indexedVars[varId]
            };

            if(groupedAttrs[varId]){
              mapping.attributes = R.indexBy(R.prop('key'), groupedAttrs[varId]);
            }
            return mapping;
          }

          return R.map(buildVariableMapping, columnMappings);
        };

        const resolveEntityMappings = columnMappings => {

          if(!columnMappings){
            return Task.rejected('Invalid Column Mappings. The Column Mappings must be provided when importing data.');
          }

          const variableIds = R.values(columnMappings);

          const vars = entityRepository.queryById(
            variable.entityType,
            variableIds);

          const attrs = entityRepository.query(
            attribute.entityType,
            q.build.isIn('variable', variableIds));

          return thread(
            R.sequence(Task.of, [ vars, attrs ]),
            R.map(([v, a]) => mergeEntitiesAndMappings(columnMappings, v, a)));
        };

        const mappingsTask = resolveEntityMappings(columnMappings);

        return mappingsTask.map(mappings => {

          var rowIndex = 0;

          const processDataRow = (reject, resolve, row) => {
            rowIndex += 1;

            const individualItem = createIndividual(
              dataSetId,
              mappings,
              rowIndex,
              row);

            individualItem.fork(reject, resolve);
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
