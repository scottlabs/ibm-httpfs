var err = false;
var user = process.env.npm_package_config_user;
if ( ! user ) {
    err = true;
    console.log('You must specify a user for testing, like');
    console.log('npm config set ibm-hdfs:user foo');
}

var password = process.env.npm_package_config_password;
if ( ! password ) {
    err = true;
    console.log('You must specify a password for testing, like');
    console.log('npm config set ibm-hdfs:password bar');
}

var url = process.env.npm_package_config_url;
if ( ! url ) {
    err = true;
    console.log('You must specify a URL for testing, like');
    console.log('npm config set ibm-hdfs:url http://www.google.com');
}

if ( err ) {
    throw "Please fix above errors and try again.";
}

module.exports = {
    user: user,
    password: password,
    url: url
}
