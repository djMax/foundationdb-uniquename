/*!
 * foundationdb-uniquename
 * Copyright(c) 2015 Max Metral <opensource@pyralis.com>
 * MIT Licensed
 */
var util = require('util'),
    EventEmitter = require('events').EventEmitter,
    FoundationDb = require('fdb').apiVersion(300);

var debug = require('debuglog')('foundationdb-uniquename');

/**
 * Default options
 */
var defaultOptions = {
    directory: 'uniquenames',
    canonicalizer: function (name) {
        return name.toLowerCase();
    }
};

/**
 * Construct a new UniqueName manager.
 * @param options
 *  @param directory The FoundationDb directory in which to store the data about names
 *  @param canonicalizer The function used to canonicalize names (defaults to lowercasing)
 * @constructor
 */
var UniqueName = function (options) {
    options = options || {};
    var directory = options.directory || defaultOptions.directory;
    this.canonicalizer = options.canonicalizer || defaultOptions.canonicalizer;

    var self = this;

    function changeState(newState) {
        debug('switched to state: %s', newState);
        self.state = newState;
        self.emit(newState);
    }

    function connectionReady(err) {
        if (err) {
            debug('not able to connect to the database');
            changeState('disconnected');
            throw err;
        }
        changeState('connected');
    }

    if (options.hasOwnProperty('fdb')) {
        this.fdb = options.fdb;
    } else {
        this.fdb = FoundationDb.open(options.clusterFile);
    }

    // You only get to call this function once.
    this.connect = function () {
        delete self.connect;
        changeState('init');
        FoundationDb.directory.createOrOpen(this.fdb, directory)(function (err, dir) {
            self.dir = dir;
            self.countKey = dir.pack(['count']);
            connectionReady(err);
        });
        changeState('connecting');
    };

    if (this.fdb) {
        this.connect();
    } else {
        // This means you plan to fill out fdb later, and then you must call connect yourself.
        changeState('disconnected');
    }
};

/**
 * In case you want to leave your names completely alone, pass
 * UniqueName.IdentityCanonicalizer in your options.
 */
UniqueName.IdentityCanonicalizer = function (x) {
    return x;
}

util.inherits(UniqueName, EventEmitter);

/**
 * Attempt to claim a name for a certain period of time (or forever)
 * @param name the name such as 'max', that will no longer be available for other ownerIds. This name
 *      will be canonicalized using the canonicalizer property of the options passed to the constructor. By default,
 *      it will be lowercased.
 * @param ownerId the owner of the name
 * @param expiration the epoch after which this name becomes available again, or null if it never expires
 * @param callback called on completion of the operation with (error, successBoolean)
 */

UniqueName.prototype.takeName = function (name, ownerId, expiration, callback) {
    if (typeof(expiration) === 'function') {
        callback = expiration;
        expiration = null;
    }
    var key = this.keyForName(name);
    var newValue = {
        id: ownerId,
        raw: name
    };
    if (expiration) {
        newValue.end = expiration;
    }
    var newValueJson = JSON.stringify(newValue);

    var success = false;
    this.fdb.doTransaction(function (tr, commit) {
        tr.get(key, function (readErr, readEntity) {
            if (readErr) {
                return commit(readErr);
            }
            var owner;
            if (readEntity) {
                try {
                    owner = JSON.parse(readEntity);
                } catch (parseError) {
                    return commit(parseError);
                }
                if (owner.id === newValue.id) {
                    // Current owner has it. Only write if expiration is later or "now set to something"
                    if (!newValue.end || (owner.end && newValue.end < owner.end)) {
                        success = true;
                        return commit();
                    }
                } else if (!owner.end || Date.now() < owner.end) {
                    // We can't have this one. Taken and not expired.
                    return commit();
                }
            }
            success = true;
            tr.set(key, newValueJson);
            commit();
        });
    }, function (trError) {
        callback(trError, success);
    });
};

/**
 * Internal method to generate the FoundationDB key for a given name
 */

UniqueName.prototype.keyForName = function (name) {
    // We put nm in front in case we/you want to extend this directory with other indexes/metadata (such as reverse lookup)
    return this.dir.pack(['nm', this.canonicalizer(name)]);
};

/**
 * Remove an owner's claim to a name, allowing another owner to claim it at some point in the future.
 * @param name
 * @param callback
 */
UniqueName.prototype.removeName = function (name, tr, callback) {
    if (typeof(tr) === 'function') {
        callback = tr;
        tr = null;
    }
    var key = this.keyForName(name);
    (tr || this.fdb).clear(key, callback);
};

/**
 * Find the current owner for the given name.
 * @param callback standard callback - second argument is an object with a Buffer for the ownerId and exp
 * for the expiration of this ownership, if any. Also contains "raw" property which is the name before
 * canonicalization.
 */
UniqueName.prototype.entityForName = function (name, tr, callback) {
    if (typeof(tr) === 'function') {
        callback = tr;
        tr = null;
    }
    (tr || this.fdb).get(this.keyForName(name), function (error, entity) {
        if (error) {
            return callback(error);
        }
        var parsed = null;
        if (entity) {
            try {
                parsed = JSON.parse(entity.toString());
            } catch (parseException) {
                return callback(parseException);
            }
        }
        callback(null, parsed);
    });
};

UniqueName.prototype.changeOwner = function (name, from, to, expiration, passedTr, callback) {
    if (typeof(expiration) === 'function') {
        callback = expiration;
        expiration = null;
    }
    if (typeof(passedTr) === 'function') {
        callback = passedTr;
        passedTr = null;
    }
    var key = this.keyForName(name);

    var success = false;

    var ownFunction = function (tr, commit) {
        tr.get(key, function (readErr, readEntity) {
            if (readErr) {
                return commit(readErr);
            }
            var owner;
            if (readEntity) {
                try {
                    owner = JSON.parse(readEntity);
                } catch (parseError) {
                    return commit(parseError);
                }
                if (owner.id === from) {
                    if (expiration) {
                        owner.end = expiration;
                    } else {
                        delete owner.end;
                    }
                    owner.id = to;
                    tr.set(key, JSON.stringify(owner));
                    success = true;
                } else if (owner.id === to) {
                    if (expiration && owner.end !== expiration) {
                        owner.end = expiration;
                        tr.set(key, JSON.stringify(owner));
                    }
                    success = true;
                }
            }
            commit();
        });
    }, invokeCallback = function (err) {
        callback(err, success);
    };

    if (passedTr) {
        ownFunction(passedTr, invokeCallback);
    } else {
        this.fdb.doTransaction(function (autoTr, autoCommit) {
            ownFunction(autoTr, autoCommit);
        }, invokeCallback);
    }
};

module.exports = UniqueName;
