const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const sinonChai = require('sinon-chai');

chai.use(sinonChai);
chai.use(chaiAsPromised);

const {expect} = chai;

const Task = require('data.task');
const Async = require('control.async')(Task);
const when = require('when');
const task2Promise = Async.toPromise(when.promise);

const sinon = require('sinon');

const {individual, fact, variable} = require('davis-model');

const queryFac = require('../../src/data/dataQuery');

describe('Data Query', function(){
  // Set up for query methods
  const stubbIt = () => {
    const queryStub = sinon.stub();
    const storage = {
      data: {
        query: queryStub
      }
    };

    const query = queryFac({
      storage,
      catalog: 'cat'
    });
    return {queryStub, storage, query};
  };

  const maFilter = [
    { variable: 1, attributes: [2], type: variable.types.categorical }
  ];

  it('should bubble up Task errors from storage.data.query', function(){
    const {queryStub, query} = stubbIt();
    queryStub.returns(Task.rejected('Error message'));
    const result = task2Promise(query([]));
    return expect(result).to.be.rejectedWith('Error message');
  });

  it('should return empty sets when no data is returned', function(){
    const {queryStub, query} = stubbIt();

    const results = [];
    queryStub.returns(Task.of(results));

    const result = task2Promise(query(maFilter, [1,2,3]));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', maFilter, [1,2,3]),
      expect(result).to.eventually.deep.equal({
        1: [],
        2: [],
        3: []
      })
    ]);
  });

  it('should merge empty sets with data', function(){
    const {queryStub, query} = stubbIt();

    const facts = [
      fact.newCategorical(1,1),
      fact.newQuantitative(2,5000)
    ];
    const results = [
      individual.new(1, 1, facts)
    ];

    queryStub.returns(Task.of(results));

    const result = task2Promise(query(maFilter, [1,2,3]));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', maFilter, [1,2,3]),
      expect(result).to.eventually.deep.equal({
        1: [facts],
        2: [],
        3: []
      })
    ]);
  });

  it('should split results by data set', function(){
    const {queryStub, query} = stubbIt();

    const facts1 = [
      fact.newCategorical(1,1),
      fact.newQuantitative(2,5000)
    ];

    const facts2 = [
      fact.newCategorical(1,2),
      fact.newQuantitative(2,45)
    ];

    const results = [
      individual.new(1, 1, facts1),
      individual.new(1, 2, facts2)
    ];

    queryStub.returns(Task.of(results));

    const result = task2Promise(query(maFilter, [1,2]));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', maFilter, [1,2]),
      expect(result).to.eventually.deep.equal({
        1: [facts1],
        2: [facts2]
      })
    ]);
  });
});
