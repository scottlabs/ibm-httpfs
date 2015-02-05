var chai = require("chai");
chai.should();
chai.use(require('chai-things'));
var params = require('./params');
var fs = require('fs');
var path = require('path');
var Q = require('q');

var HttpFS = require('../index');

describe('HttpFS', function() {
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
        HttpFS.bind(null).should.throw();

        HttpFS.bind(null, {}).should.throw();

        HttpFS.bind(null, {
            user: 'foo'
        }).should.throw();

        HttpFS.bind(null, {
            user: 'foo',
            password: 'bar'
        }).should.throw();

        HttpFS.bind(null, {
            url: 'http://www.google.com:80',
            password: 'bar'
        }).should.throw();
    });

    it('should throw an error if no port is provided', function() {
        HttpFS.bind(null, {
            user : 'foo',
            password : 'bar',
            url : 'http://www.google.com'
        }).should.throw();
    });

    it('should create a new HttpFS', function() {

        var httpfs = new HttpFS({
            user : 'foo',
            password : 'bar',
            url : 'https://bi-hadoop-prod-xxx.services.dal.bluemix.net:14443/webhttpfs/v1/'
        });

        httpfs.should.be.ok();
    });

    it('should catch a login error', function(done) {
        this.timeout(5000);
        httpfs = new HttpFS({
            user: 'foo',
            password: params.password,
            url: params.url
        });

        httpfs.listDirectory('foo').fail(function(err) {
            err.should.be.ok();
            err.message.should.equal('Login failed');
            err.exception.should.equal('LoginFailed');
            done();
        });
    });

    it('should parse errors', function(done) {
        this.timeout(10000);
        httpfs = new HttpFS(params);
        httpfs.listDirectory('foo').fail(function(err) {
            err.should.be.ok();
            err.message.indexOf('does not exist').should.be.above(-1);
            err.exception.should.equal('FileNotFoundException');
            done();
        });
    });

    it('should report particulars of access control exceptions, when permissions fail', function(done) {
        this.timeout(20000);
        var remoteFile = 'foo'; // this file won't work

        httpfs = new HttpFS(params);
        httpfs.upload(remoteFile, 'bar').fail(function(err) {
            err.args.user.should.equal(params.user);
            err.args.access.should.equal('WRITE');
            done();
        });
    });

    describe('List directory', function() {
        // HttpFS can be a li'l on the slow side
        this.timeout(20000);
        var httpfs;

        before(function() {
            httpfs = new HttpFS(params);
        });

        it('should list a directory', function(done) {
            httpfs.listDirectory('user').then(function(results) {
                results.should.be.ok();
                results.length.should.be.above(0);
                done();
            });
        });
    });

    describe('Create directory', function() {
        // HttpFS can be a li'l on the slow side
        this.timeout(20000);
        var httpfs;

        before(function() {
            httpfs = new HttpFS(params);
        });

        it('should create a directory', function(done) {
            var rootDir = 'tmp/';
            var dir = 'foo'+(new Date()).getTime();
            httpfs.createDirectory(rootDir+dir).then(function(results) {
                return httpfs.listDirectory(rootDir);
            }).then(function(dirs) {
                var exists = false;
                dirs.map(function(fileName) {
                    if ( fileName.pathSuffix === dir ) {
                        exists = true;
                    }
                });
                exists.should.equal(true);
                return httpfs.remove(rootDir+dir);
            }).then(function() {
                done();
            }).fail(done);
        });

        it('should show an error if you don\'t have permissions', function(done) {
            var dir = 'foo'+(new Date()).getTime();
            httpfs.createDirectory(dir).fail(function(err){
                err.exception.should.equal('AccessControlException');
                done();
            });
        });
    });

    describe('Upload', function() {
        // HttpFS can be a li'l on the slow side
        this.timeout(20000);
        var httpfs;
        var remoteFiles = [
            files.string.remote.split('/').pop(),
            files.file.remote.split('/').pop(),
            files.big.remote.split('/').pop()
        ];

        before(function(done) {
            httpfs = new HttpFS(params);
            done();
        });

        after(function(done) {
            httpfs.listDirectory('tmp').then(function(files) {
                var promises = [];
                files.map(function(file) {
                    var fileName = file.pathSuffix;
                    if ( remoteFiles.indexOf(fileName) !== -1 ) {
                        promises.push(
                            function() {
                                return httpfs.remove(fileName)
                            }
                        );
                    }
                });
                return Q.all(promises);
            }).then(function() {
                done();
            }).fail(done);
        });

        it('should upload a string', function(done) {
            httpfs.upload(files.string.remote, files.string.contents).then(function(body, response) {
                return httpfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.should.contain.a.thing.with.property('pathSuffix', files.string.name);
                done();
            }).fail(done);
        });

        it('should upload a file', function(done) {
            httpfs.upload(files.file.remote, files.file.contents).then(function(body, response) {
                return httpfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.should.contain.a.thing.with.property('pathSuffix', files.file.name);
                done();
            }).fail(done);
        });

        it('should upload a big file', function(done) {
            // big file
            this.timeout(60000);
            httpfs.upload(files.big.remote, files.big.contents).then(function(body, response) {
                return httpfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.should.contain.a.thing.with.property('pathSuffix', files.big.name);
                done();
            }).fail(done);
        });
    });

    describe('Download', function() {
        // HttpFS can be a li'l on the slow side
        this.timeout(20000);
        var httpfs;

        before(function(done) {
            httpfs = new HttpFS(params);
            httpfs.upload(files.string.remote, files.string.contents).then(function(body, response) {
                done();
            }).fail(done);
        });

        after(function(done) {
            httpfs.remove(files.string.remote).then(function() {
                done();
            }).fail(done);
        });

        it('should download a file\'s contents', function(done) {
            httpfs.download(files.string.remote).then(function(response) {
                response.should.equal(files.string.contents);
                done();
            }).fail(done);
        });

        it('should save the file locally', function(done) {
            var localFile = 'tmp.txt';
            httpfs.download(files.string.remote, localFile).then(function() {
                var contents = fs.readFileSync(localFile, 'utf8');
                contents.should.equal(files.string.contents);
                fs.unlink(localFile, function() {
                    done();
                });
            }).fail(done);
        });
    });

    describe('Remove file', function() {
        // HttpFS can be a li'l on the slow side
        this.timeout(20000);
        var httpfs;
        var fileContent = 'foo';
        var remoteFile = 'tmp/foo';

        before(function(done) {
            httpfs = new HttpFS(params);
            httpfs.upload(remoteFile, fileContent).then(function(body, response) {
                done();
            }).fail(done);
        });

        it('should remove a remote file', function(done) {
            httpfs.remove(remoteFile).then(function() {
                return httpfs.listDirectory('tmp');
            }).then(function(results) {
                results.should.be.ok();
                results.should.not.contain.a.thing.with.property('pathSuffix', remoteFile.split('/').pop());
                done();
            }).fail(done);
        });
    });
});
