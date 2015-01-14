# foundationdb-uniquename
[![Build Status](https://travis-ci.org/djMax/foundationdb-uniquename.png)](https://travis-ci.org/djMax/foundationdb-uniquename)

A simple module to manage a set of unique names in FoundationDb. This can be useful for users choosing aliases on a
website, or unique phone numbers, etc.

```js
    var UniqueNames = require('foundationdb-uniquename');
    var usernames = new UniqueNames({
        directory: 'usernames'
    });
    usernames.takeName('djMax', 'user_id_or_guid_or_something', function (error, success) {
        // If success is true and error is null, this user now owns the name.
        assert(success, 'takeName should have worked');
    });

```