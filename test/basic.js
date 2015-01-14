'use strict';
var assert = require('assert'),
    cb = require('assert-called'),
    FoundationDb = require('fdb').apiVersion(300),
    UniqueNames = require('..');

require('longjohn');

describe('connect-foundationdb', function () {

    var mydb, fs, range;

    var eat = function (done, fn) {
        return function (e, data) {
            try {
                assert.ifError(e);
                if (fn) {
                    fn(data);
                }
                done();
            } catch (x) {
                done(x);
            }
        }
    };

    // Cleanup any existing collection
    before(function (done) {
        mydb = FoundationDb.open();
        FoundationDb.directory.removeIfExists(mydb, 'test-names')(function before(e) {
            console.log('Completed test setup.');
            done(e);
        });
    });

    it('should create the UniqueNames object', function createStore(done) {
        fs = new UniqueNames({
            directory: 'test-names'
        });
        fs.on('connected', cb(function (e) {
            FoundationDb.directory.open(mydb, 'test-names')(eat(done, function (d) {
                assert(d, 'Should have a test-sessions directory.');
                range = d.range();
            }));
        }))
    });

    it('should take a name', function getName(done) {
       fs.takeName('djMax', 'user1', eat(done, function (success) {
           assert(success, 'takeName should have worked');
       }));
    });

    it('should lookup the name', function lookupName(done) {
        fs.entityForName('DJMAX', eat(done, function (entity) {
            assert(entity.raw === 'djMax');
        }));
    });

    it('should not take the same name with a different owner', function getSameName(done) {
        fs.takeName('djMax', 'user2', null, eat(done, function (success) {
            assert(!success, 'takeName should not have worked');
        }));
    });

    it('should get an existing name for the same user', function getSameNameSameUser(done) {
        fs.takeName('djMax', 'user1', null, eat(done, function (success) {
            assert(success, 'takeName should have worked');
        }));
    });

    it('should remove an existing name', function removeName(done) {
        fs.removeName('djMax', eat(done));
    });

    it('should not find the removed name', function dontFindName(done) {
        fs.entityForName('DJMAX', eat(done, function (entity) {
            assert(!entity);
        }));
    });

    it('should take a name with expiration', function getNameAgain(done) {
        fs.takeName('djMax', 'user1', Date.now() - 100000, eat(done, function (success) {
            assert(success, 'takeName should have worked');
        }));
    });

    it('should take a name that has expired', function getExpiredName(done) {
        fs.takeName('djMax', 'user2', eat(done, function (success) {
            assert(success, 'takeName should have worked');
        }));
    });

    it('should fail to get the name now that it isn\'t expired', function getNameAgain(done) {
        fs.takeName('djMax', 'user1', eat(done, function (success) {
            assert(!success, 'takeName should not have worked');
        }));
    });

});