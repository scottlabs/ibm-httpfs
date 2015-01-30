var chai = require("chai");
chai.should();
chai.use(require('chai-things'));
var params = require('./params');
var fs = require('fs');

var HDFS = require('../index');

describe('HDFS', function() {
    var stringName = 'string.txt';
    var fileName = 'file.txt';
    var remoteStringName = 'tmp/string.txt';
    var remoteFileName = 'tmp/file.txt';
    var str = 'fooBar';

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
            hdfs.upload(remoteStringName, str).then(function(body, response) {
                return hdfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.FileStatuses.should.be.ok();
                results.FileStatuses.FileStatus.should.be.ok();
                var files = results.FileStatuses.FileStatus;

                //console.log('results', JSON.stringify(files, null, 2));
                files.should.contain.a.thing.with.property('pathSuffix', stringName);
                done();
            }).fail(done);
        });

        it('should upload a file', function(done) {
            var localFile = 'fixtures/'+fileName;
            hdfs.upload(remoteFileName, localFile).then(function(body, response) {
                return hdfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.FileStatuses.should.be.ok();
                results.FileStatuses.FileStatus.should.be.ok();
                var files = results.FileStatuses.FileStatus;

                files.should.contain.a.thing.with.property('pathSuffix', fileName);
                done();
            }).fail(done);
        });
    });

    describe('Download', function() {
        // HDFS can be a li'l on the slow side
        this.timeout(20000);
        var hdfs;

        before(function() {
            hdfs = new HDFS(params);
        });

        it('should download a file\'s contents', function(done) {
            hdfs.download(remoteStringName).then(function(response) {
                response.should.equal(str);
                done();
            }).fail(done);
        });

        it('should save the file locally', function(done) {
            var localFile = 'tmp.txt';
            hdfs.download(remoteStringName, localFile).then(function() {
                var contents = fs.readFileSync(localFile, 'utf8');
                contents.should.equal(str);
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
            hdfs.remove(remoteStringName).then(function() {
                return hdfs.remove(remoteFileName);
            }).then(function() {
                return hdfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.FileStatuses.should.be.ok();
                results.FileStatuses.FileStatus.should.be.ok();
                var files = results.FileStatuses.FileStatus;

                files.should.not.contain.a.thing.with.property('pathSuffix', fileName);
                files.should.not.contain.a.thing.with.property('pathSuffix', stringName);
                done();
            }).fail(done);
        });
    });

});
