const sinon = require('sinon');
const Task = require('data.task');
const R = require('ramda');
const shared = require('davis-shared');
const model = require('davis-model');
const {parse, build: q} = model.query;
const {dataSet, folder, variable} = model;
const {toArray} = shared.array;
const {thread} = shared.fp;
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const {expect} = chai;
chai.use(chaiAsPromised);
const Async = require('control.async')(Task);
const when = require('when');
const task2Promise = Async.toPromise(when.promise);

const buildEntityRepositoryStub = entities => {

  const entitiesByType = R.groupBy(R.prop('entityType'), entities);

  const queryAll = sinon.stub()
    .callsFake((type) => Task.of(
      entitiesByType[type] || []));

  const queryById = sinon.stub()
    .callsFake((type, ids) => thread(
      queryAll(type),
      R.map(R.filter(e => R.any(id => e.id === id, toArray(ids))))));

  // Recursively builds a filter predicate function the query definition.
  //
  // Examples
  //
  // Exp: ["=", "id", 1]
  // Fn: x => x.id === 1;
  //
  // Exp: ["and",
  //   ["=", "foo", "bar"],
  //   [">", "date", "1/2017"]
  // ]
  // Fn: R.allPass([
  //   R.propEq('foo', 'bar'),  // Using ramda in this example
  //   x => x.date > '1/2017'])
  //
  // Exp: ["or",
  //   ["and",
  //     ["=", "foo", "bar"],
  //     [">", "date", "1/2017"]
  //   ],
  //   ["<", "date", "1/2016"]
  // ]
  // Fn: R.anyPass([
  //   R.allPass([
  //     R.propEq('foo', 'bar'),
  //     x => x.date > '1/2017']),
  //   x => x.date < '1/2016'])
  const buildFilterPred = expression => {

    const op = parse.op(expression);
    const args = R.tail(expression);

    if(op.type === 'logical') {

      if(op.symbol === 'not'){
        return R.complement(buildFilterPred(args[0])); // should only have 1 arg
      }
      else if(op.symbol === 'nor'){
        return R.allPass(args.map(R.pipe(
          buildFilterPred,
          R.complement)));
      }
      else if(op.symbol === 'or'){
        return R.anyPass(args.map(buildFilterPred));
      }
      else if(op.symbol === 'and'){
        return R.allPass(args.map(buildFilterPred));
      }
      else{
        throw `Invalid operator: ${op.symbol}`;
      }
    }
    else if(op.symbol === '='){
      return x => x[args[0]] === args[1];
    }
    else if(op.symbol === '!='){
      return x => x[args[0]] !== args[1];
    }
    else if(op.symbol === '<'){
      return x => x[args[0]] < args[1];
    }
    else if(op.symbol === '<='){
      return x => x[args[0]] <= args[1];
    }
    else if(op.symbol === '>'){
      return x => x[args[0]] > args[1];
    }
    else if(op.symbol === '>='){
      return x => x[args[0]] >= args[1];
    }
    else if(op.symbol === 'like'){
      return x => R.test(new RegExp(`${args[1]}`), x[args[0]]);
    }
    else if(op.symbol === 'in'){
      return x => R.any(val => x[args[0]] === val, args[1]);
    }
    else if(op.symbol === 'notin'){
      return R.complement(x => R.any(val => x[args[0]] === val, args[1]));
    }
    else{
      throw `Invalid operator: ${op.symbol}`;
    }
  };

  const query = sinon.stub()
    .callsFake((type, query) => thread(
      queryAll(type),
      R.map(R.filter(buildFilterPred(query)))));

  return {
    queryAll,
    queryById,
    query,
    create: sinon.stub()
  };
};

describe('Entity Repository Stub', function(){

  describe('queryAll', function(){

    const ents = [
      folder.new(1, 'F1'),
      folder.new(2, 'F2'),
      dataSet.new(1, 'ds1'),
      dataSet.new(2, 'ds2')
    ];

    const repo = buildEntityRepositoryStub(ents);

    it('should return all of a type', function(){
      const result = task2Promise(repo.queryAll(folder.entityType));
      return expect(result).to.eventually.deep.equal([
        ents[0],
        ents[1]]);
    });

    it('should return empty set for no entities', function(){
      const result = task2Promise(repo.queryAll(variable.entityType));
      return expect(result).to.eventually.deep.equal([]);
    });
  });

  describe('queryById', function(){

    const ents = [
      folder.new(1, 'F1'),
      folder.new(2, 'F2')
    ];

    const repo = buildEntityRepositoryStub(ents);

    it('should return entity of id', function(){
      const result = task2Promise(repo.queryById(folder.entityType, 2));
      return expect(result).to.eventually.deep.equal([ents[1]]);
    });

    it('should return empty set for no matches in existing entity set', function(){
      const result = task2Promise(repo.queryById(folder.entityType, 5));
      return expect(result).to.eventually.deep.equal([]);
    });

    it('should return empty set for no matches in missing entity set', function(){
      const result = task2Promise(repo.queryById(variable.entityType, 5));
      return expect(result).to.eventually.deep.equal([]);
    });
  });

  describe('query', function(){

    const ents = [
      folder.new(1, 'F1'),
      folder.new(2, 'F2'),
      folder.new(40, 'F40'),
      folder.new(41, 'F40')
    ];

    const repo = buildEntityRepositoryStub(ents);

    it('should match = query', function(){
      const result = task2Promise(repo.query(folder.entityType, q.eq('id', 2)));
      return expect(result).to.eventually.deep.equal([ents[1]]);
    });

    it('should match != query', function(){
      const result = task2Promise(repo.query(folder.entityType, q.neq('id', 2)));
      return expect(result).to.eventually.deep.equal([ents[0], ents[2], ents[3]]);
    });

    it('should match > query', function(){
      const result = task2Promise(repo.query(folder.entityType, q.gt('id', 2)));
      return expect(result).to.eventually.deep.equal([ents[2], ents[3]]);
    });

    it('should match >= query', function(){
      const result = task2Promise(repo.query(folder.entityType, q.gte('id', 2)));
      return expect(result).to.eventually.deep.equal([ents[1], ents[2], ents[3]]);
    });

    it('should match < query', function(){
      const result = task2Promise(repo.query(folder.entityType, q.lt('id', 2)));
      return expect(result).to.eventually.deep.equal([ents[0]]);
    });

    it('should match <= query', function(){
      const result = task2Promise(repo.query(folder.entityType, q.lte('id', 2)));
      return expect(result).to.eventually.deep.equal([ents[0], ents[1]]);
    });

    it('should match like query', function(){
      const result = task2Promise(repo.query(folder.entityType, q.like('name', '2')));
      return expect(result).to.eventually.deep.equal([ents[1]]);
    });

    it('should match in query', function(){
      const result = task2Promise(repo.query(folder.entityType, q.in('id', [1,2])));
      return expect(result).to.eventually.deep.equal([ents[0], ents[1]]);
    });

    it('should match not in query', function(){
      const result = task2Promise(repo.query(folder.entityType, q.nin('id', [2])));
      return expect(result).to.eventually.deep.equal([ents[0], ents[2], ents[3]]);
    });

    it('should build and query', function(){
      const result = task2Promise(repo.query(folder.entityType,
        q.and(q.eq('name', 'F40'), q.lt('id', 41))));
      return expect(result).to.eventually.deep.equal([ents[2]]);
    });

    it('should build or query', function(){
      const result = task2Promise(repo.query(folder.entityType,
        q.or(q.eq('name', 'F40'), q.eq('id', 2))));
      return expect(result).to.eventually.deep.equal([ents[1], ents[2], ents[3]]);
    });

    it('should build not query', function(){
      const result = task2Promise(repo.query(folder.entityType,
        q.not(q.eq('name', 'F40'))));
      return expect(result).to.eventually.deep.equal([ents[0], ents[1]]);
    });

    it('should build nor query', function(){
      const result = task2Promise(repo.query(folder.entityType,
        q.nor(q.eq('name', 'F40'), q.eq('id', 1))));
      return expect(result).to.eventually.deep.equal([ents[1]]);
    });
  });
});

module.exports = buildEntityRepositoryStub;
