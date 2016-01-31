/**
 * Created by nitesh on 1/27/16.
 */

var _ = require("underscore");
var config  = require("../config");
var log     = require("../logger");
var Promise = require("bluebird");
var zkConnection = require("../framework/src/zkConnection");
Promise.promisifyAll(zkConnection, {multiArgs: true});

process.title = "NODE-ZK";

var meetingAwareDiscovery;
var meetingAwarePath = "/bjn/test/seam/meeting.services";
var interval    = config.get("EXECUTION:INTERVAL");
var perInterval = config.get("EXECUTION:PER_INTERVAL");
var nInterval   = config.get("EXECUTION:N_INTERVAL");

var zkSetup = function() {
    return new Promise(function (resolve, reject) {
        zkConnection.init({}, function (err) {
            if (err) {
                log.error("zkSetup: Failed to initialize zookeeper ", err);
                return reject(err);
            }
            return resolve(zkConnection);
        });
    })
};

zkSetup().then(function (zkConnection) {
    var zkConnection1 = require("../framework/src/zkConnection");
    log.info("zkSetup is successfull, sessionid = ", zkConnection.connection.sessionId());
});
