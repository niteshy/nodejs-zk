var _ = require('underscore');
var zookeeper = require('node-zookeeper-client');
var log = require('../../logger');

var HEALTHCHECK_TIMEOUT = 9000; // 9sec
var SESSION_TIMEOUT = 60000; // 60 sec

var zclient;

function init(opts, cb) {
    if(_.isFunction(opts)) {
        cb = opts;
        opts = {};
    }
    var options = _.defaults(opts, {sessionTimeout: SESSION_TIMEOUT});
    log.debug("using options", options);
    zclient = zookeeper.createClient(process.env.ZOOKEEPER_HOSTS, opts);
    log.info("Zookeeper:%s: connecting to hosts - ", sessionId(), process.env.ZOOKEEPER_HOSTS);

    zclient.once('connected', function() {
        log.info('Zookeeper:%s: connected negotiated timeout sessionTimeout = %d', sessionId(), zclient.getSessionTimeout());
        return cb(null, zclient);
    });

    zclient.on('state', function(st) {
        log.info("Zookeeper:%s: state", sessionId(), st);

        if(st === zookeeper.State.DISCONNECTED) {
            log.warn("Zookeeper:%s: session disconnected; Will wait for the network to be available", sessionId());
        }
        if(st === zookeeper.State.EXPIRED) {
            log.warn("Zookeeper:%s: session expired; reconnecting..", sessionId());
            zclient.close();
            init(opts, function (err) {
                if(err) {
                    return log.error("Zookeeper:%s: Reconnecting on expired state, failed", sessionId(), err);
                }
                return log.info("Zookeeper:%s: Reconnection on the expired state: successful", sessionId());
            });
        }
    });
    zclient.connect();
    // wait for the connected event to come. if it does not come in the specified period, just say we could not connect and let it fail
    setTimeout(function () {
        if(!isConnected()) {
            log.error("Zookeeper:%s: connection timedout in %s ms", sessionId(), HEALTHCHECK_TIMEOUT);
            return cb("ZK connection timed out in "+ HEALTHCHECK_TIMEOUT + " ms");
        }
    }, HEALTHCHECK_TIMEOUT);
}

function healthcheck(cb) {
    var serviceBasePath = '/bjn/'+ process.env.DENIM_PARTITION_NAME + '/seam/services';
    var status = false;
    // if zk getData failed to respond with HEALTHCHECK_TIMEOUT
    var healthCheckTimeoutTimer = setTimeout(function () {
        if (!status) {
            var msg = "Zookeeper: healthCheck timeout";
            log.error({state: zclient.getState() || "DISCONNECTED", disconnected: true, reason: msg});
            return cb({state: zclient.getState() || "DISCONNECTED", disconnected: true, reason: msg});
        }
    }, HEALTHCHECK_TIMEOUT);
    zclient.getData(serviceBasePath, function (err, data, stat) {
        clearTimeout(healthCheckTimeoutTimer);
        if (err || !data) {
            var errmsg = "Zookeeper: healthCheck failed to read services node data, err - " + err;
            log.error({state: zclient.getState() || "DISCONNECTED", disconnected: true, reason: msg});
            return cb({state: zclient.getState() || "DISCONNECTED", disconnected: true, reason: msg});
        }
        return cb(null, {state: zclient.getState(), sid: sessionId(), sessionTimeout: zclient.getSessionTimeout()});
    });
}

function isConnected() {
    var state = zclient.getState();
    return state === zookeeper.State.SYNC_CONNECTED ||
        state === zookeeper.State.CONNECTED ||
        state === zookeeper.State.SASL_AUTHENTICATED;
}

function sessionId() {
    var sid = zclient.getSessionId();
    if(sid) {
        return sid.toString('hex');
    }
    return "No SessionId";
}

function getState() {
    return zclient.getState();
}

module.exports = {
    init: init,
    healthcheck: healthcheck,
    connection: {isConnected: isConnected, state: getState, sessionId: sessionId}
};
