const { isNil } = require('ramda');

module.exports = function(formatters){

  return {
    format: function(variable, value){

      if(isNil(value) || isNaN(value)){
        return '';
      }

      if(!variable.format){
        return String(value);
      }

      var formatter = formatters[variable.format.type];

      if(!formatter){
        throw `Invalid format type, or missing formatter: ${variable.format.type}`;
      }

      return formatter(value, variable.format.options);
    }
  };
};
