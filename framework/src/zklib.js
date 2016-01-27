"use strict";

var Promise = require('bluebird');
var _ = require('underscore');
var zkPath = require('../lib/zkPath');

module.exports = function(zclient) {
    Promise.promisifyAll(zclient, {multiArgs: true});
    var l = {
        rmr: function removeRecursive(path) {
            // patch the path and remove the leaves and go upward.
            console.log("Removing root node", path);
            return zclient.existsAsync(path).then(function (exists) {
                if(exists) {
                    return zclient.getChildrenAsync(path).spread(function (children, stats) {
                        console.log(" the root", path, "has ", children);
                        return Promise.each(children, function (child) {
                            return removeRecursive(zkPath.join(path,child));
                        });
                    }).then(function removeRoot() {
                        console.log("removing the root", path);
                        return zclient.removeAsync(path);
                    });
                } else {
                    // nothing to be done.
                    return Promise.resolve();
                }
            });
        },
        getAndWatchNodeData: function (path, watcher, x) {
            return zclient.getDataAsync(path, function  dataWatcher(event) {
                console.log("data watcher for " + path);
                if(event.name === 'NODE_DELETED') {
                    // no need to register the watch. Just get it out of the list
                    console.log("node with " + event.path + " has been deleted removing from the list");
                    x.children = _.reject(x.children, function (c) {
                        return x.path === event.path;
                    });
                } else {
                    l.getAndWatchNodeData(path, watcher, x);
                }
                if(watcher) {
                    watcher(event);
                }

            }).spread(function (data, stat) {
                if(data) {
                    x.data = data.toString('utf-8');
                } else {
                    x.data = null; // explicitly reset
                }
                return [data, stat];
            });
        },
        getAndWatchNodeChildren: function (path, options, watcher, x) {
            return zclient.getChildrenAsync(path, function childrenwatcher (event){
                console.log("Children Watch", event);
                if(event.name !== 'NODE_DELETED') {
                    l.getAndWatchNodeChildren(event.path, options, watcher, x);
                }
                if(watcher) {
                    watcher(event);
                }

            }).get(0).map(function(child) {
                return l.addSelfAndChildWatcher(zkPath.join(path, child), options, watcher);
            }).then(function (all) {
                x.children = all;
            });
        },
        // this is not the same as get children and watch on the parent, it will actually get all children, and then apply
        // watcher on each of the children.
        // following options can be provided
        // {times: 1, added: true, deleted: true, recursive: true} or {times: Infinity} this will keep watching the node/nodes forever. Default is once only(just like regular zookeeper watches)
        addSelfAndChildWatcher: function (path, options, onWatch) {
            var x = {path: path, data: null, children: []};
            return l.getAndWatchNodeData(path, onWatch, x)
                .then(function (arg) {
                    return l.getAndWatchNodeChildren(path, options, onWatch, x);
                }).return(x);
        },

        watchAllChildren: function (path, options, onWatch) {
            if(arguments.length === 2 && _.isFunction(options)){
                onWatch = options;
                options = {times: 1};
            }
            return l.addSelfAndChildWatcher(path, options, onWatch).tap(console.log);
        }
     };
    return l;
};
