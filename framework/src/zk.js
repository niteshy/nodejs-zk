var _ = require('underscore');
var util = require('util');
var zookeeper = require('node-zookeeper-client');
var log = require('../../logger');
var events = require("events");
var EventEmitter = require("events").EventEmitter;

var HEALTHCHECK_TIMEOUT = 3000; // 3sec
var zkWrapper = new EventEmitter();

function init(opts, cb) {
    if(_.isFunction(opts)) {
        cb = opts;
        opts = {};

    }
    var options = _.defaults(opts, {sessionTimeout: 60000});
    log.debug("using options", options);
    zkWrapper.zclient = zookeeper.createClient(process.env.ZOOKEEPER_HOSTS, opts);
    log.info("Zookeeper: connecting to hosts - " + process.env.ZOOKEEPER_HOSTS);

    //zkWrapper.zclient.once('connected', function() {
    //    log.info('Zookeeper:%s: connected negotiated timeout sessionTimeout = %d', sessionId(), zkWrapper.zclient.getSessionTimeout());
    //    return cb(null, zkWrapper);
    //});

    var reinit = function () {
        zkWrapper.zclient.close();
        init(opts, function (err) {
            if(err) {
                return log.error("Zookeeper:%s: Reconnecting on expired state, failed", sessionId(), err);
            }
            return log.info("Zookeeper:%s: Reconnection on the expired state: successful", sessionId());
        });
    };

    zkWrapper.zclient.on('state', function(st) {
        log.info("Zookeeper:%s: state", sessionId(), st);
        if (st === zookeeper.State.AUTH_FAILED) {
            zkWrapper.emit("AUTH_FAILED");
            log.error("Zookeeper:%s: AUTH_FAILED", sessionId());
            return cb("ZK connection AUTH_FAILED");
        }
        if (st === zookeeper.State.SASL_AUTHENTICATED) {
            zkWrapper.emit("SASL_AUTHENTICATED");
        }
        if (st === zookeeper.State.SYNC_CONNECTED) {
            zkWrapper.emit("SYNC_CONNECTED");
            log.info('Zookeeper:%s: connected negotiated timeout sessionTimeout = %d', sessionId(), zkWrapper.zclient.getSessionTimeout());
            return cb(null, zkWrapper);
        }
        if (st === zookeeper.State.CONNECTED_READ_ONLY) {
            zkWrapper.emit("CONNECTED_READ_ONLY");
            log.info('Zookeeper:%s: connected_read_only negotiated timeout sessionTimeout = %d', sessionId(), zkWrapper.zclient.getSessionTimeout());
        }

        if(st === zookeeper.State.DISCONNECTED) {
            zkWrapper.emit("DISCONNECTED");
            log.warn("Zookeeper:%s: session disconnected; Will wait for the network to be available", sessionId());
        }
        if(st === zookeeper.State.EXPIRED) {
            zkWrapper.emit("EXPIRED");
            log.warn("Zookeeper:%s: session expired; reconnecting..", sessionId());
            reinit();
        }
    });
    zkWrapper.zclient.connect();
    // wait for the connected event to come. if it does not come in the specified period, just say we could not connect and let it fail
    setTimeout(function () {
        if(!isConnected()) {
            zkWrapper.zclient.close();
            return cb("ZK connection timed out in " + HEALTHCHECK_TIMEOUT + " ms");
        }
    }, HEALTHCHECK_TIMEOUT);
}

function healthcheck(cb) {
    var serviceBasePath = '/bjn/'+ process.env.DENIM_PARTITION_NAME + '/seam/services';

    var timerExecuted = false;
    // if zk getData failed to respond with HEALTHCHECK_TIMEOUT
    var healthCheckTimeoutTimer = setTimeout(function () {
        var msg = "Zookeeper: healthCheck timeout in " + HEALTHCHECK_TIMEOUT + " ms";
        timerExecuted = true;
        return cb({state: zkWrapper.zclient.getState(), reason: msg});
    }, HEALTHCHECK_TIMEOUT);
    return zkWrapper.zclient.getData(serviceBasePath, function (err, data, stat) {
        if (!timerExecuted) {
            clearTimeout(healthCheckTimeoutTimer);
            if (err || !data) {
                var errmsg = "Zookeeper: healthCheck failed to read services node data, err - " + err;
                log.error("Zookeeper: getData failed ", {state: zkWrapper.zclient.getState(), reason: errmsg});
                return cb({state: zkWrapper.zclient.getState(), reason: errmsg});
            } else {
                return cb(null, {state: zkWrapper.zclient.getState(), sid: sessionId(), sessionTimeout: zkWrapper.zclient.getSessionTimeout()});
            }
        }
    });
}

function isConnected() {
    var state = zkWrapper.zclient.getState();
    return state === zookeeper.State.SYNC_CONNECTED ||
        state === zookeeper.State.CONNECTED ||
        state === zookeeper.State.SASL_AUTHENTICATED;
}
function sessionId() {
    var sid = zkWrapper.zclient.getSessionId();
    if(sid) {
        return sid.toString('hex');
    }
    return "No SessionId";
}
function getState() {
    return zkWrapper.zclient.getState();
}

module.exports = {
    init: init,
    healthcheck: healthcheck,
    connection: {isConnected: isConnected, state: getState, sessionId: sessionId}
};
