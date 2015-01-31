var chai = require("chai");
chai.should();
chai.use(require('chai-things'));
var params = require('./params');
var fs = require('fs');
var path = require('path');

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
                done();
            });
        });
    });

    describe('Upload', function() {
        // HDFS can be a li'l on the slow side
        this.timeout(20000);
        var hdfs;

        before(function() {
            hdfs = new HDFS(params);
        });

        it('should upload a string', function(done) {
            hdfs.upload(files.string.remote, files.string.contents).then(function(body, response) {
                //return hdfs.listDirectory('tmp');
            //}).then(function(results) {
                //results.should.be.ok();
                //results.FileStatuses.should.be.ok();
                //results.FileStatuses.FileStatus.should.be.ok();
                //results.FileStatuses.FileStatus.should.contain.a.thing.with.property('pathSuffix', files.string.name);
                done();
            }).fail(done);
        });

        it.only('should upload a file', function(done) {
            hdfs.upload(files.file.remote, files.file.contents).then(function(body, response) {
                //return hdfs.listDirectory('tmp');
            //}).then(function(results) {
                //results.should.be.ok();
                //results.FileStatuses.should.be.ok();
                //results.FileStatuses.FileStatus.should.be.ok();
                //results.FileStatuses.FileStatus.should.contain.a.thing.with.property('pathSuffix', files.file.name);
                done();
            }).fail(done);
        });

        /*
        it('should upload a big file', function(done) {
            // big file
            this.timeout(60000);
            hdfs.upload(files.big.remote, files.big.contents).then(function(body, response) {
                return hdfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.FileStatuses.should.be.ok();
                results.FileStatuses.FileStatus.should.be.ok();
                results.FileStatuses.FileStatus.should.contain.a.thing.with.property('pathSuffix', files.big.name);
                return hdfs.remove(files.big.remote);
            }).then(function() {
                done();
            }).fail(done);
        });
        */
    });

    describe('Download', function() {
        // HDFS can be a li'l on the slow side
        this.timeout(20000);
        var hdfs;

        before(function() {
            hdfs = new HDFS(params);
        });

        it('should download a file\'s contents', function(done) {
            hdfs.download(files.file.remote).then(function(response) {
                var contents = fs.readFileSync(files.file.contents, 'utf8');
                response.should.equal(contents);
                done();
            }).fail(done);
        });

        it('should save the file locally', function(done) {
            var localFile = 'tmp.txt';
            var origContent = fs.readFileSync(files.file.contents, 'utf8');
            hdfs.download(files.file.remote, localFile).then(function() {
                var contents = fs.readFileSync(localFile, 'utf8');
                contents.should.equal(origContent);
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

        before(function() {
            hdfs = new HDFS(params);
        });

        it('should remove a remote file', function(done) {
            hdfs.remove(files.string.remote).then(function() {
                return hdfs.remove(files.file.remote);
            }).then(function() {
                return hdfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.FileStatuses.should.be.ok();
                results.FileStatuses.FileStatus.should.be.ok();
                var files = results.FileStatuses.FileStatus;

                files.should.not.contain.a.thing.with.property('pathSuffix', files.file.name);
                files.should.not.contain.a.thing.with.property('pathSuffix', files.string.name);
                done();
            }).fail(done);
        });
    });

});
