const R = require('ramda');
const model = require('davis-model');
const {user} = model;
const q = model.query.build;
const shared = require('davis-shared');
const {crypto, fp} = shared;
const {either2Task, thread} = fp;
const Task = require('data.task');

module.exports = ({
  storage,
  catalog,
  config
}) =>
{
  const encKey = new Buffer(config.crypto.encryptionKey, 'hex');
  const valKey = new Buffer(config.crypto.validationKey, 'hex');

  const decode = crypto.decode(encKey, valKey);
  const encode = crypto.encode(encKey, valKey);

  const getUser = results => results.length > 0 ?
    Task.of(results[0]) :
    Task.rejected('User not found.');

  const userById = id => thread(
    storage.entities.query(
      catalog,
      user.entityType,
      q.eq('id', id)),
    R.chain(getUser));

  const userByEmail = email => thread(
    storage.entities.query(
      catalog,
      user.entityType,
      q.eq('email', email)),
    R.chain(getUser));

  const userByToken = R.pipe(
    decode,
    either2Task,
    R.chain(({userId}) => userById(userId)));

  const validatePassword = R.curry((password, u) =>
    user.comparePassword(password, u) ?
    Task.of(u) :
    Task.rejected('Incorrect password'));

  const generateToken = u => encode({ userId: u.id });

  const login = (email, password) =>
    // Query for the user by email
    userByEmail(email)
  // Check the password
      .chain(validatePassword(password))
  // Generate an auth token for this user
      .map(generateToken);

  return {
    userByToken,
    login
  };
};
