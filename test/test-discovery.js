/**
 * Created by nitesh on 27/01/16.
 */
var config  = require("../config");
var log     = require("../logger");
var zkConnection = require("../framework/src/zkConnection");

process.title = "NODE-ZK:CON";

var zkSetup = function(cb) {
    return zkConnection.init(function (err, zclient) {
        if (err) {
            log.error("Failed to initialize zookeeper");
            return cb(err);
        }
        return cb(null, zclient);
    });
};

var resources = {};

zkSetup(function(err, zclient) {
    if (err) {
        log.error("zkSetup failed");
    }
    var discovery = require('../framework/recipes/discovery')(zclient, {basePath: "/bjn/test/seam/services"});
    resources.zclient = zclient;
    resources.discovery= discovery;
    return discovery.registerService('event.service.addr', config.get("MY_NODE_IP"), config.get("MY_PORT"), function (err, selfPath) {
        if(err) {
            log.error("Failed to register event service into zookeeper");
            return callback(err);
        }
        log.info("It's all been done");
        setInterval(function () {
            meetingAwareDiscovery = require('../framework/recipes/meetingAwareDiscovery')(zclient, {basePath: meetingAwarePath});
        }, 1000);
        return selfPath;
    });
});
