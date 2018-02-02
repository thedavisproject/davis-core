const R = require('ramda');
const util = require('davis-shared');
const { thread } = util.fp;
const { isNilOrEmpty } = util.string;
const model = require('davis-model');
const q = model.query.build;
const { variable, attribute } = model;
const Task = require('data.task');
const Set = require('Set');

module.exports =
  ({
    entityRepository
  }) =>
  {
    const matchVariableInList = R.curry((dataSetId, variables, key) => {
      const firstScoped = R.find(
        v => v.key === key && v.scopedDataSet === dataSetId,
        variables);

      return firstScoped || R.find(v => v.key === key, variables);
    });

    return (dataSetId, dataStream, {
      limit = 0,
      column = null
    } = {}) => {

      var columns;

      const initColumns = R.pipe(
          R.indexBy(R.identity),
          R.map(key => ({
            key: key,
            values: new Set()
          })));

      function processColumn(key, value){
        if(isNilOrEmpty(value)){
          return;
        }
        if(columns[key].values.size() < limit){
          columns[key].values.add(value);
        }
      }

      const processData = R.curry((rejectHandler, d) => {

        // For single column analyze
        if(column){
          if(!R.contains(column, R.keys(d))){
            rejectHandler(new Error(`Data file does not contain column ${column}`));
          }
          if(!columns){
            columns = initColumns([column]);
          }

          processColumn(column, d[column]);
        }
        // For full analyze
        else {
          if(!columns){
            columns = initColumns(R.keys(d));
          }

          R.keys(d).forEach(k => processColumn(k, d[k]));
        }
      });

      const tryToLocateVariables = keys => thread(
        entityRepository.query(variable.entityType, q.in('key', keys)),
        R.map(R.filter(v => !v.scopedDataSet || v.scopedDataSet === dataSetId)),
        R.map(vars => keys.map(matchVariableInList(dataSetId, vars))),
        R.map(R.filter(v => !R.isNil(v))),
        R.map(R.indexBy(R.prop('key'))));

      const tryToLocateAttributes = R.pipe(
        R.map(v => entityRepository.query(attribute.entityType,
          q.and(
            q.eq('variable', v.variable.id),
            q.in('key', v.attributeKeys)))),
        R.sequence(Task.of),
        R.map(R.flatten),
        R.map(R.groupBy(R.prop('variable'))),
        R.map(R.map(R.indexBy(R.prop('key')))));

      function buildVariableMatch(
        existingVariables,
        existingAttributes,
        column){

        const v = existingVariables[column.key];

        var vOut = v ? {
          key: column.key,
          match: true,
          variable: v.id
        } : {
          key: column.key,
          match: false
        };

        if(v && v.type === variable.types.categorical){

          var variableAttributes = existingAttributes[v.id] ?
            existingAttributes[v.id] : {};

          vOut.values = column.values.toArray()
            .map(a => {
              const attr = variableAttributes[a];

              if(attr){
                return {
                  value: a,
                  match: true,
                  attribute: attr.id
                };
              }

              return {
                value: a,
                match: false
              };
            });
        }
        else {
          vOut.values = column.values.toArray()
            .map(a => {
              return {
                value: a
              };
            });
        }

        return vOut;
      }

      function matchVariablesAndAttributes(columns){

        const resolvedVariables = tryToLocateVariables(R.keys(columns));

        const resolvedAttributes = resolvedVariables
          .chain(vars => thread(
            R.values(columns),
            // Has an existing var and it is categorical
            R.filter(c => vars[c.key] && vars[c.key].type === variable.types.categorical),
            R.map(c => ({
              variable: vars[c.key],
              attributeKeys: c.values.toArray()
            })),
            tryToLocateAttributes));

        return R.sequence(Task.of, [
          resolvedVariables,
          resolvedAttributes
        ]);
      }

      const parseAllData = new Task(function(reject, resolve){
        dataStream
          .on('data', processData(reject))
          .on('error', reject)
          .on('end', function(){
            resolve(columns);
          });
      });

      return thread(
        parseAllData,
        R.chain(columns =>
          matchVariablesAndAttributes(columns)
            .map(([vars, attrs]) =>
              R.values(columns).map(c =>
                buildVariableMatch(vars, attrs, c)))));
    };
  };
