
module.exports = {
  data: {
    export: {
      csvExport: require('./src/data/export/csvExport')
    },
    import: {
      parse: {
        csvParser: require('./src/data/import/parse/csvParser')
      },
      dataAnalyze: require('./src/data/import/dataAnalyze'),
      individualGenerator: require('./src/data/import/individualGenerator'),
      dataImport: require('./src/data/import/dataImport')
    },
    dataDelete: require('./src/data/dataDelete'),
    dataQuery: require('./src/data/dataQuery')
  },
  entities: {
    entityRepository: require('./src/entities/entityRepository')
  },
  format: {
    dataFormatter: require('./src/format/dataFormatter'),
    defaultFormatters: {
      percent: require('./src/format/percentFormat'),
      number: require('./src/format/numberFormat')
    }
  },
  publish: require('./src/publish/publish')
};
