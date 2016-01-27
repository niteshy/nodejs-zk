/**
 * Created by nitesh on 27/01/16.
 */

var conf = require('nconf').argv().env().file({ file: __dirname+'/config.json' });
module.exports = conf;