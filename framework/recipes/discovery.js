"use strict";
var log = require('../../logger');
var zookeeper = require('node-zookeeper-client');
var _ = require('underscore');
var util = require('util');
var zk = require('../src/zk');
var zkPath = require('../lib/zkPath');


module.exports = function (zkWrapper, opts) {
    var zklib = require('../src/zklib')(zkWrapper);
    var options = _.defaults( opts || {}, {basePath:'/bjn/' + process.env.DENIM_PARTITION_NAME + '/seam/services'});
    var services = null;



    var discovery = {
        // persistent servicepath but ephemeral path for the service
        // also reregisters on zk exipiration.
        registerService: function (serviceName, ip, port, cb) {
            var servicePath = options.basePath + '/' + serviceName;
            discovery.selfPath = servicePath + '/' + ip + ':'+ port;

            var createServiceNode = function createServiceNode(cb) {
                return zkWrapper.zclient.create(discovery.selfPath, new Buffer(JSON.stringify({
                    pid: process.pid,
                    url: util.format("http://%s:%s", ip, port)
                })), zookeeper.CreateMode.EPHEMERAL, function(err) {
                    if(err && err.name !== 'NODE_EXISTS') {
                        return cb(err);
                    }
                    if (err && err.name === "NODE_EXISTS") {
                        log.info("Zookeeper: Node %s exists already, deleting and recreating.", discovery.selfPath);
                        return zkWrapper.zclient.remove(discovery.selfPath, function  (err, res) {
                            if(err) {
                                log.warn("failed to remove existing self node.");
                                return cb(err);
                            }
                            return createServiceNode(cb);
                        });
                        //setTimeout(create_zpath_node, 1000);
                    }
                    log.info('Zookeeper: Node: %s is successfully created.', discovery.selfPath);
                    return cb(null, discovery.selfPath);
                });
            };

            return zkWrapper.zclient.mkdirp(servicePath, new Buffer(ip), zookeeper.CreateMode.PERSISTENT, function(err) {
                if (err && err.name !== "NODE_EXISTS") {
                    log.info('Zookeeper: failed to create node: %s due to: %s.', servicePath, err);
                    return cb(err);
                }
                return createServiceNode(function (err, res) {
                    if(err) {
                        return cb(err);
                    }
                    // register the hook to add self back
                    zkWrapper.on('SYNC_CONNECTED', function () {
                        log.warn("Discovery: The node had disconnected, but now that the connection is restored, we are registrering the service.");
                        createServiceNode(function (err, res) {
                            if(err) {
                                return log.error("Discovery: Failed to restore event service on zookeeper reconnect");
                            }
                            return log.info("Discovery: successfully restored event service on zookeeper reconnect");
                        });
                    });
                    zklib.watchAllChildren(options.basePath, {}, function(event) {
                        log.info('Got event', event);
                    }).then(function(s) {
                        log.info("watchAllChildren services = ", s);
                        services = s;
                    });
                    return cb(err, res);
                });
            });

        },
        lookup: function (serviceName) {
            if(!zk.connection.isConnected()) {
                log.warn("Discovery: The zookeeper client is in disconnected state, the cache might be stale");
            }
            if (! services || ! services.children) {
                log.warn("Discovery: Services discovery tree is empty. Services = ", services);
                return null;
            }
            var serviceNode = _.find(services.children, function (s) {
                return zkPath.childNode(s.path) === serviceName;
            });
            log.info("Discovery lookup Found ", serviceNode);
            var serviceNodeInstances = serviceNode ? serviceNode.children : [];
            if(!_.isEmpty(serviceNodeInstances)) {
                return JSON.parse(serviceNodeInstances[_.random(serviceNodeInstances.length - 1)].data).url;
            } else {
                log.warn("No services for "+ serviceName + " was found");
                throw new Error("LookupFailure");
            }
        }
    };

    return discovery;
};
