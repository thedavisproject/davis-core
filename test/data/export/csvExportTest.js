const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');

chai.use(chaiAsPromised);

const {expect} = chai;

const Task = require('data.task');
const Async = require('control.async')(Task);
const when = require('when');

const task2Promise = Async.toPromise(when.promise);

const csvExportFac = require('../../../src/data/export/csvExport');
const { dataSet, variable, attribute, individual, fact } = require('davis-model');
const entityRepoStub = require('../../stub/entityRepositoryStub');

describe('Export', function(){
  const testEntities = [
    dataSet.new(2, 'My DatSet'),
    variable.newCategorical(5, '5-key', { key: '5-key' }),
    variable.newQuantitative(7, '7-key', { key: '7-key' }),
    attribute.new(6, '6-key', 5, { key: '6-key' })];

  it('should export single csv', function(){
    const queryStub = () => Task.of([
      {
        dataSet: 2,
        data: [
          individual.new(1, 2, [
            fact.newCategorical(5, 6),
            fact.newQuantitative(7, 34.5)
          ])
        ]
      }]);

    const entityRepo = entityRepoStub(testEntities);
    const exporter = csvExportFac({
      dataQuery: queryStub,
      entityRepository: entityRepo
    });
    const results = task2Promise(exporter.export('anything'));

    return when.all([
      expect(results).to.eventually.have.length(1),
      expect(results.then(r => r[0].dataSet)).to.eventually.deep.equal(testEntities[0]),
      expect(results.then(r => r[0].csv)).to.eventually.equal('5-key,7-key\n6-key,34.5\n')
    ]);
  });

  it('should export multiple csvs', function(){
    const testEntities = [
      dataSet.new(1, 'My DatSet One'),
      dataSet.new(2, 'My DatSet Two'),
      variable.newQuantitative(7, '7-key', { key: '7-key' })];

    const queryStub = () => Task.of([
      {
        dataSet: 1,
        data: [
          individual.new(1, 1, [
            fact.newQuantitative(7, 34.5)
          ])
        ]
      },
      {
        dataSet: 2,
        data: [
          individual.new(1, 2, [
            fact.newQuantitative(7, 100.5)
          ])
        ]
      }
    ]);

    const entityRepo = entityRepoStub(testEntities);
    const exporter = csvExportFac({
      dataQuery: queryStub,
      entityRepository: entityRepo
    });
    const results = task2Promise(exporter.export('anything'));

    return when.all([
      expect(results).to.eventually.have.length(2),
      expect(results.then(r => r[0].dataSet)).to.eventually.deep.equal(testEntities[0]),
      expect(results.then(r => r[0].csv)).to.eventually.equal('7-key\n34.5\n'),
      expect(results.then(r => r[1].dataSet)).to.eventually.deep.equal(testEntities[1]),
      expect(results.then(r => r[1].csv)).to.eventually.equal('7-key\n100.5\n')
    ]);
  });

  it('should export multiple rows with different fact orders', function(){
    const testEntities = [
      dataSet.new(1, 'My DatSet One'),
      variable.newCategorical(5, '5-key', { key: '5-key' }),
      variable.newQuantitative(7, '7-key', { key: '7-key' }),
      attribute.new(6, '6-key', { key: '6-key' }),
      attribute.new(7, '7-key', { key: '7-key' }),
      attribute.new(8, '8-key', { key: '8-key' })];

    const queryStub = () => Task.of([
      {
        dataSet: 1,
        data: [
          individual.new(1, 1, [
            fact.newCategorical(5, 6),
            fact.newQuantitative(7, 34.5)
          ]),
          individual.new(2, 1, [
            fact.newCategorical(5, 7),
            fact.newQuantitative(7, 44.5)
          ]),
          individual.new(3, 1, [
            fact.newQuantitative(7, 54.5),
            fact.newCategorical(5, 8)
          ])
        ]
      }
    ]);

    const entityRepo = entityRepoStub(testEntities);
    const exporter = csvExportFac({
      dataQuery: queryStub,
      entityRepository: entityRepo
    });
    const results = task2Promise(exporter.export('anything'));

    return when.all([
      expect(results).to.eventually.have.length(1),
      expect(results.then(r => r[0].dataSet)).to.eventually.deep.equal(testEntities[0]),
      expect(results.then(r => r[0].csv)).to.eventually.equal('5-key,7-key\n6-key,34.5\n7-key,44.5\n8-key,54.5\n')
    ]);
  });

  it('should export null attributes', function(){
    const testEntities = [
      dataSet.new(1, 'My DatSet One'),
      variable.newCategorical(5, '5-key', { key: '5-key' }),
      variable.newQuantitative(7, '7-key', { key: '7-key' }),
      attribute.new(6, '6-key', { key: '6-key' }),
      attribute.new(7, '7-key', { key: '7-key' }),
      attribute.new(8, '8-key', { key: '8-key' })];

    const queryStub = () => Task.of([
      {
        dataSet: 1,
        data: [
          individual.new(1, 1, [
            fact.newCategorical(5, null),
            fact.newQuantitative(7, NaN)
          ])
        ]
      }
    ]);

    const entityRepo = entityRepoStub(testEntities);
    const exporter = csvExportFac({
      dataQuery: queryStub,
      entityRepository: entityRepo
    });
    const results = task2Promise(exporter.export('anything'));

    return when.all([
      expect(results).to.eventually.have.length(1),
      expect(results.then(r => r[0].dataSet)).to.eventually.deep.equal(testEntities[0]),
      expect(results.then(r => r[0].csv)).to.eventually.equal('5-key,7-key\n,\n')
    ]);
  });
});
