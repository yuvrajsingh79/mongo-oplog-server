'use strict';

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

function _objectWithoutProperties(obj, keys) { var target = {}; for (var i in obj) { if (keys.indexOf(i) >= 0) continue; if (!Object.prototype.hasOwnProperty.call(obj, i)) continue; target[i] = obj[i]; } return target; }

const Emitter = require('eventemitter3');

var _require = require('mongodb');

const MongoClient = _require.MongoClient;

const createDebug = require('debug');
const createFilter = require('./filter');
const createStream = require('./stream');

const MONGO_URI = 'mongodb://127.0.0.1:27017/local';
const debug = createDebug('mongo-oplog');

const events = {
  i: 'insert',
  u: 'update',
  d: 'delete'

  // Add callback support to promise
};const toCb = fn => cb => {
  try {
    const val = fn(cb);
    if (!cb) return val;else if (val && typeof val.then === 'function') {
      return val.then(val => cb(null, val)).catch(cb);
    }
    cb(null, val);
  } catch (err) {
    cb(err);
  }
};

module.exports = (uri, options = {}) => {
  let connect = (() => {
    var _ref = _asyncToGenerator(function* () {
      if (connected) return db;
      db = yield MongoClient.connect(uri, opts);
      connected = true;
    });

    return function connect() {
      return _ref.apply(this, arguments);
    };
  })();

  let tail = (() => {
    var _ref2 = _asyncToGenerator(function* () {
      try {
        debug('Connected to oplog database');
        yield connect();
        stream = yield createStream({ ns, coll, ts, db });
        stream.on('end', onend);
        stream.on('data', ondata);
        stream.on('error', onerror);
        return stream;
      } catch (err) {
        onerror(err);
      }
    });

    return function tail() {
      return _ref2.apply(this, arguments);
    };
  })();

  let stop = (() => {
    var _ref3 = _asyncToGenerator(function* () {
      if (stream) stream.destroy();
      debug('streaming stopped');
      return oplog;
    });

    return function stop() {
      return _ref3.apply(this, arguments);
    };
  })();

  let destroy = (() => {
    var _ref4 = _asyncToGenerator(function* () {
      yield stop();
      if (!connected) return oplog;
      yield db.close(true);
      connected = false;
      return oplog;
    });

    return function destroy() {
      return _ref4.apply(this, arguments);
    };
  })();

  let db;
  let stream;
  let connected = false;

  const ns = options.ns,
        since = options.since,
        coll = options.coll,
        opts = _objectWithoutProperties(options, ['ns', 'since', 'coll']);

  const oplog = new Emitter();

  let ts = since || 0;
  uri = uri || MONGO_URI;

  if (typeof uri !== 'string') {
    if (uri && uri.collection) {
      db = uri;
      connected = true;
    } else {
      throw new Error('Invalid mongo db.');
    }
  }

  function filter(ns) {
    return createFilter(ns, oplog);
  }

  function ondata(doc) {
    if (oplog.ignore) return oplog;
    debug('incoming data %j', doc);
    ts = doc.ts;
    oplog.emit('op', doc);
    oplog.emit(events[doc.op], doc);
    return oplog;
  }

  function onend() {
    debug('stream ended');
    oplog.emit('end');
    return oplog;
  }

  function onerror(err) {
    if (/cursor (killed or )?timed out/.test(err.message)) {
      debug('cursor timeout - re-tailing %j', err);
      tail();
    } else {
      debug('oplog error %j', err);
      oplog.emit('error', err);
    }
  }

  return Object.assign(oplog, {
    db,
    filter,
    tail: toCb(tail),
    stop: toCb(stop),
    destroy: toCb(destroy)
  });
};

module.exports.events = events;
module.exports.default = module.exports;