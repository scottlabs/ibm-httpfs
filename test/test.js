var chai = require("chai");
chai.should();
chai.use(require('chai-things'));
var params = require('./params');
var fs = require('fs');
var path = require('path');
var Q = require('q');

var HDFS = require('../index');

describe('HDFS', function() {
    var files = {
        string: {
            name: 'string.txt',
            remote: 'tmp/string.txt',
            contents: 'fooBar'
        },
        file: {
            name: 'file.txt',
            remote: 'tmp/file.txt',
            contents: path.resolve('test/fixtures/file.txt')
        },
        big: {
            name: 'bigger.txt',
            remote: 'tmp/bigger.txt',
            contents: path.resolve('test/fixtures/bigger.txt')
        }
    }

    it('should throw an error if missing arguments', function() {
        HDFS.bind(null).should.throw();

        HDFS.bind(null, {}).should.throw();

        HDFS.bind(null, {
            user: 'foo'
        }).should.throw();

        HDFS.bind(null, {
            user: 'foo',
            password: 'bar'
        }).should.throw();

        HDFS.bind(null, {
            url: 'http://www.google.com:80',
            password: 'bar'
        }).should.throw();
    });

    it('should throw an error if no port is provided', function() {
        HDFS.bind(null, {
            user : 'foo',
            password : 'bar',
            url : 'http://www.google.com'
        }).should.throw();
    });

    it('should create a new HDFS', function() {

        var hdfs = new HDFS({
            user : 'foo',
            password : 'bar',
            url : 'https://bi-hadoop-prod-xxx.services.dal.bluemix.net:14443/webhdfs/v1/'
        });

        hdfs.should.be.ok();
    });

    it('should catch a login error', function(done) {
        this.timeout(5000);
        hdfs = new HDFS({
            user: 'foo',
            password: params.password,
            url: params.url
        });

        hdfs.listDirectory('foo').fail(function(err) {
            err.should.be.ok();
            err.message.should.equal('Login failed');
            err.exception.should.equal('LoginFailed');
            done();
        });
    });

    it('should parse errors', function(done) {
        this.timeout(10000);
        hdfs = new HDFS(params);
        hdfs.listDirectory('foo').fail(function(err) {
            err.should.be.ok();
            err.message.indexOf('does not exist').should.be.above(-1);
            err.exception.should.equal('FileNotFoundException');
            done();
        });
    });

    it('should report particulars of access control exceptions, when permissions fail', function(done) {
        this.timeout(20000);
        var remoteFile = 'foo'; // this file won't work

        hdfs = new HDFS(params);
        hdfs.upload(remoteFile, 'bar').fail(function(err) {
            err.args.user.should.equal(params.user);
            err.args.access.should.equal('WRITE');
            done();
        });
    });

    describe('List directory', function() {
        // HDFS can be a li'l on the slow side
        this.timeout(20000);
        var hdfs;

        before(function() {
            hdfs = new HDFS(params);
        });

        it('should list a directory', function(done) {
            hdfs.listDirectory('user').then(function(results) {
                results.should.be.ok();
                results.length.should.be.above(0);
                done();
            });
        });
    });

    describe('Upload', function() {
        // HDFS can be a li'l on the slow side
        this.timeout(20000);
        var hdfs;
        var uploadedString;
        var uploadedFile;

        before(function() {
            hdfs = new HDFS(params);
        });

        after(function() {
            Q.fcall(function() {
                if ( uploadedString ) {
                    return hdfs.remove(files.string.remote);
                }
            }).then(function() {
                if ( uploadedFile ) {
                    return hdfs.remove(files.file.remote);
                }
            }).then(function() {
                done();
            });
        });

        it('should upload a string', function(done) {
            hdfs.upload(files.string.remote, files.string.contents).then(function(body, response) {
                return hdfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.should.contain.a.thing.with.property('pathSuffix', files.string.name);
                uploadedString = true;
                done();
            }).fail(done);
        });

        it('should upload a file', function(done) {
            hdfs.upload(files.file.remote, files.file.contents).then(function(body, response) {
                return hdfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.should.contain.a.thing.with.property('pathSuffix', files.file.name);
                uploadedFile = true;
                done();
            }).fail(done);
        });

        it('should upload a big file', function(done) {
            // big file
            this.timeout(60000);
            hdfs.upload(files.big.remote, files.big.contents).then(function(body, response) {
                return hdfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.should.contain.a.thing.with.property('pathSuffix', files.big.name);
                return hdfs.remove(files.big.remote);
            }).then(function() {
                done();
            }).fail(done);
        });
    });

    describe('Download', function() {
        // HDFS can be a li'l on the slow side
        this.timeout(20000);
        var hdfs;

        before(function(done) {
            hdfs = new HDFS(params);
            hdfs.upload(files.string.remote, files.string.contents).then(function(body, response) {
                done();
            }).fail(done);
        });

        after(function(done) {
            hdfs.remove(files.string.remote).then(function() {
                done();
            }).fail(done);
        });

        it('should download a file\'s contents', function(done) {
            hdfs.download(files.string.remote).then(function(response) {
                response.should.equal(files.string.contents);
                done();
            }).fail(done);
        });

        it('should save the file locally', function(done) {
            var localFile = 'tmp.txt';
            hdfs.download(files.string.remote, localFile).then(function() {
                var contents = fs.readFileSync(localFile, 'utf8');
                contents.should.equal(files.string.contents);
                fs.unlink(localFile, function() {
                    done();
                });
            }).fail(done);
        });
    });

    describe('Remove file', function() {
        // HDFS can be a li'l on the slow side
        this.timeout(20000);
        var hdfs;
        var fileContent = 'foo';
        var remoteFile = 'tmp/foo';

        before(function(done) {
            hdfs = new HDFS(params);
            hdfs.upload(remoteFile, fileContent).then(function(body, response) {
                done();
            }).fail(done);
        });

        it('should remove a remote file', function(done) {
            hdfs.remove(remoteFile).then(function() {
                return hdfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.should.not.contain.a.thing.with.property('pathSuffix', remoteFile.split('/').pop());
                done();
            }).fail(done);
        });
    });

});
