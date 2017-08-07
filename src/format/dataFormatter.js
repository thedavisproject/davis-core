const { isNil } = require('ramda');
const { variable } = require('davis-model');

module.exports = function(formatters){

  return {
    format: function(variableWithValue, value){

      if(isNil(value) ||
         (variableWithValue.type === variable.types.numerical && isNaN(value))){
        return '';
      }

      if(!variableWithValue.format){
        return String(value);
      }

      var formatter = formatters[variableWithValue.format.type];

      if(!formatter){
        throw `Invalid format type, or missing formatter: ${variableWithValue.format.type}`;
      }

      return formatter(value, variableWithValue.format.options);
    }
  };
};
