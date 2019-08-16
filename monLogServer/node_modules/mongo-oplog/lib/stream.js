'use strict';

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

var _require = require('mongodb');

const Timestamp = _require.Timestamp;

var _require2 = require('./filter');

const regex = _require2.regex;


module.exports = (() => {
  var _ref = _asyncToGenerator(function* ({ db, ns, ts, coll }) {
    let time = (() => {
      var _ref2 = _asyncToGenerator(function* () {
        if (ts) return typeof ts !== 'number' ? ts : Timestamp(0, ts);

        const doc = yield coll.find({}, { ts: 1 }).sort({ $natural: -1 }).limit(1).nextObject();

        return doc ? doc.ts : Timestamp(0, Date.now() / 1000 | 0);
      });

      return function time() {
        return _ref2.apply(this, arguments);
      };
    })();

    if (!db) throw new Error('Mongo db is missing.');

    const query = {};

    coll = db.collection(coll || 'oplog.rs');

    if (ns) query.ns = { $regex: regex(ns) };
    query.ts = { $gt: yield time() };

    return (yield coll.find(query, {
      tailable: true,
      awaitData: true,
      oplogReplay: true,
      noCursorTimeout: true,
      numberOfRetries: Number.MAX_VALUE
    })).stream();
  });

  return function (_x) {
    return _ref.apply(this, arguments);
  };
})();