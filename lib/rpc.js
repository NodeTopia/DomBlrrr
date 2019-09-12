const HTTP = require('./transports/http');
const KBucket = require('k-bucket');
const events = require('events');
const randombytes = require('randombytes');
const util = require('util');
const async = require('async');
const simpleSha1 = require('simple-sha1');
const low = require('last-one-wins');
const LRU = require('lru');

const K = 20;
const MAX_CONCURRENCY = 16;
const BOOTSTRAP_NODES = [];
const BUCKET_OUTDATED_TIMESPAN = 30 * 1000 // check nodes in bucket in 15 minutes old buckets

function noop() { };


class RPC extends events.EventEmitter {
    constructor(opts) {
        super();

        if (!(this instanceof RPC))
            return new RPC(opts);

        this._idLength = opts.idLength || 20;
        this.id = (opts.id || opts.nodeId || randombytes(this._idLength));

        this.socket = opts.krpcSocket || new HTTP(Object.assign(opts, { id: this.id }));
        this.socket.onQuery = this.onQuery.bind(this);
        this.socket.on('listening', this.onListening.bind(this))

        this.bootstrap = RPC.toBootstrapArray(opts.nodes || opts.bootstrap);
        this.concurrency = opts.concurrency || MAX_CONCURRENCY;
        this.backgroundConcurrency = opts.backgroundConcurrency || (this.concurrency / 4) | 0;
        this.k = opts.k || K;

        this._bucketOutdatedTimeSpan = opts.timeBucketOutdated || BUCKET_OUTDATED_TIMESPAN;
        this._runningBucketCheck = false;
        this._bucketCheckTimeout = null;

        this.destroyed = false;

        this.nodes = null;

        this.tables = new LRU({ max: opts.maxValues || 1000 });

        this.commands = {};

        this.bind = this.socket.bind.bind(this.socket);

        this.clear();

    }


    static toBootstrapArray(val) {
        if (val === false)
            return [];
        return [].concat(val || BOOTSTRAP_NODES).map(RPC.parsePeer);
    }
    static parsePeer(peer) {
        if (typeof peer === 'string')
            return {
                host: peer.split(':')[0],
                port: Number(peer.split(':')[1])
            };
        return peer;
    }



    updateBucketTimestamp() {
        this.nodes.metadata.lastChange = Date.now();
    }

    onListening() {
        let self = this;

        this.populate('_find_node', {
            target: this.id
        }).then(onPopulation).catch(onPopulation);

        this.emit('listening');

        console.log(this.id.toString('hex').substring(0, 6), 'onListening')
        function onPopulation() {
            self.updateBucketTimestamp();
            self._setBucketCheckInterval();
            self.emit('ready');
        }
    }


    clear() {
        var self = this;

        this.nodes = new KBucket({
            localNodeId: this.id,
            numberOfNodesPerKBucket: this.k,
            numberOfNodesToPing: this.concurrency
        });
        var onping = low(ping)

        this.nodes.on('ping', function (oldContacts, newContact) {
            onping({
                older: oldContacts, swap: function (diedNode) {
                    if (!deadNode)
                        return;
                    if (deadNode.id)
                        self.nodes.remove(deadNode.id);
                    self.addNode(null, newContact);
                }
            });
        });
        function ping(opts, cb) {
            let older = opts.older;
            let swap = opts.swap;

            self._checkNodes(older, false, function (_, deadNode) {
                if (deadNode) {
                    swap(deadNode);
                    return cb();
                }
                cb();
            });
        }

    }

    onQuery(query, rinfo) {
        let self = this;

        const { id, method, data } = query;

        this.addNode(id, rinfo);

        return new Promise(async function (resolve, reject) {

            let result;

            try {
                switch (method) {
                    case '_ping':
                        result = await self.onPring(query, rinfo);
                        break;
                    case '_find_node':
                        result = await self._onFindNode(query, rinfo);
                        break;
                    case '_find_peers':
                        result = await self._onFindPeers(query, rinfo);
                        break;
                    default:
                        result = await self._onCommand(query, rinfo);
                }
            } catch (err) {

                var error = {};

                Object.getOwnPropertyNames(err).forEach(function (key) {
                    error[key] = err[key];
                }, err);

                return reject(error);
            }
            resolve(result);

        });


    }
    onPring(query, rinfo) {
        return {
            pong: true
        };
    }

    _onFindPeers(query, rinfo) {

        const { id, data } = query;
        const { target } = data;

        let table = this.tables.get(target.toString('hex'));

        if (table) {
            return {
                target: target,
                nodes: [],
                peers: table.table.toArray().concat({
                    id: this.id,
                    host: this.socket.host,
                    port: this.socket.port
                })
            };
        } else {
            return {
                target: target,
                nodes: this.nodes.closest(target),
                peers: []
            };
        }
    }

    _onFindNode(query, rinfo) {

        const { id, data } = query;

        this.addNode(id, rinfo)
        if (data.nodes && Array.isArray(data.nodes)) {
            data.nodes.forEach(this.addNode.bind(this, null))
        }

        if (!validateId(data.target)) return {}

        return {
            target: data.target,
            nodes: this.nodes.closest(data.target, this.k).concat({
                id: this.id,
                port: this.socket.port,
                host: this.socket.host
            })
        }
    }
    _onCommand(query, rinfo) {

        const { id, method, data } = query;
        const { target } = data;

        let table = this.tables.get(target.toString('hex'));

        if (table) {

            let command = table.methods.find(function (command) {
                return command[0] == method;
            });

            if (!command) {
                throw new Error('method not found')
            }

            const [m, func, scope] = command;

            return func.call(scope, data, rinfo);


        } else {
            throw new Error('not found')
        }


    }

    _setBucketCheckInterval() {
        let self = this;
        const interval = 1 * 60 * 1000; // check age of bucket every minute

        this._runningBucketCheck = true;
        queueNext();

        function checkBucket() {
            const diff = Date.now() - self.nodes.metadata.lastChange;

            if (diff < self._bucketOutdatedTimeSpan)
                return queueNext();

            self._pingAll(function () {
                if (self.destroyed) return

                if (self.nodes.toArray().length < 1) {
                    // node is currently isolated,
                    // retry with initial bootstrap nodes
                    //self._bootstrap(true);
                }

                queueNext();
            });
        }

        function queueNext() {
            if (!self._runningBucketCheck || self.destroyed)
                return;

            let nextTimeout = Math.floor(Math.random() * interval + interval / 2);
            self._bucketCheckTimeout = setTimeout(checkBucket, nextTimeout);
        }
    }
    _pingAll(cb) {
        this._checkAndRemoveNodes(this.nodes.toArray(), cb)
    }
    _sendPing(node, cb) {

        let self = this;
        let expectedId = node.id;


        function onResponce([err, res, node]) {
            if (err)
                return cb(err);
            if (!node.id || !Buffer.isBuffer(node.id)) {
                return cb(new Error('Bad reply'));
            }
            if (Buffer.isBuffer(expectedId) && !expectedId.equals(node.id)) {
                return cb(new Error('Unexpected node id'));
            }

            self.updateBucketTimestamp();

            cb(null, {
                id: node.id,
                host: node.host,
                port: node.port
            });
        }

        this.query('_ping', {}, node).then(onResponce).catch(onResponce);

    }

    _checkAndRemoveNodes(nodes, cb) {
        let self = this

        this._checkNodes(nodes, true, function (_, node) {
            if (node)
                self.removeNode(node.id);
            cb(null, node);
        })
    }

    _checkNodes(nodes, force, cb) {
        let self = this;

        test(nodes);

        function test(acc) {
            var current = null;

            while (acc.length) {
                current = acc.pop();
                if (!current.id || force)
                    break;
                if (Date.now() - (current.seen || 0) > 10000)
                    break; // not pinged within 10s
                current = null;
            }

            if (!current)
                return cb(null);

            self._sendPing(current, function (err) {
                if (!err) {
                    self.updateBucketTimestamp();
                    return test(acc);
                }
                cb(null, current);
            });
        }
    }

    removeNode(node) {
        this.nodes.remove(node.id);
    }

    addNode(id, node) {
        if (!id) {
            if (node.id) {
                id = node.id;
            } else {
                return;
            }
        }

        if (id.toString('hex') == this.id.toString('hex'))
            return;

        let old = this.nodes.get(id);

        if (old) {
            old.seen = Date.now();
            return;
        }

        this.nodes.add({
            id: id,
            port: node.port,
            host: node.host,
            distance: 0,
            seen: Date.now()
        });

        console.log('%s found node %s (target: %s)', this.id.toString('hex').substring(0, 6), id.toString('hex').substring(0, 6), `${node.host}:${node.port}`);
        this.emit('node', this.nodes.get(id));
    }

    closest(method, message, onreply) {
        return this._closest(method, message, false, onreply);
    }

    populate(method, message) {
        return this._closest(method, message, true, null);
    }

    _closest(method, message = {}, background, visit) {

        let target = message.target || sha1(method);

        let self = this;
        let count = 0;
        let queried = {};
        let pending = 0;
        let once = true;
        let stop = false;


        let table = new KBucket({
            localNodeId: target,
            numberOfNodesPerKBucket: this.k,
            numberOfNodesToPing: this.concurrency
        });

        return new Promise(function (resolve, reject) {

            kick();

            function kick() {
                if (self.destroyed || self.socket.inflight >= self.concurrency)
                    return;


                var closest = table.closest(target, self.k);
                if (!closest.length || closest.length < self.bootstrap.length) {
                    closest = self.nodes.closest(target, self.k);
                    if (!closest.length || closest.length < self.bootstrap.length)
                        bootstrap();
                }


                for (var i = 0; i < closest.length; i++) {
                    if (stop) break;
                    if (self.socket.inflight >= self.concurrency) return;

                    var peer = closest[i];
                    var id = peer.host + ':' + peer.port;

                    if (queried[id])
                        continue;

                    queried[id] = true;

                    pending++;

                    self.query(method, message, peer).then(onQuery).catch(onQuery);
                }

                if (!pending) {
                    process.nextTick(done)
                }
            }
            function done() {
                console.log(`found ${count} peers `)
                resolve(count);
            }

            function onQuery([err, res, node]) {
                pending--
                if (node) queried[node.host + ':' + node.port] = true // need this for bootstrap nodes



                if (err && node && node.id) {
                    //self.nodes.remove(node.id);
                }

                if (!res) {
                    return kick();
                }

                if (!err && node && node.id) {
                    count++;
                    add({
                        id: node.id,
                        port: node.port,
                        host: node.host,
                        distance: 0
                    });
                }


                let nodes = res.nodes ? res.nodes : [];
                for (var i = 0; i < nodes.length; i++) add(nodes[i]);

                try {
                    if (visit && visit(res, node) === false) stop = true;
                } catch (e) {
                    throw e
                }


                kick();
            }


            function bootstrap() {
                if (!once)
                    return;
                once = false;
                self.bootstrap.forEach(function (node) {
                    pending++;
                    self.query(method, message, node).then(onQuery).catch(onQuery);
                });
            }
            function add(node) {
                if (node.id.equals(self.id))
                    return;
                table.add(node);
            }
        });
    }

    async query(method, data, node) {
        //console.log(`querying node ${node.host}:${node.port} method ${method}`)


        let query = {
            id: this.id,
            method: method,
            data: data
        };

        let [err, res, rinfo] = await this.socket.send(query, node);

        this.addNode(null, rinfo);

        return [err, res, rinfo];

    }

    addTable(table) {


        const { target } = table;

        let old = this.tables.get(target.toString('hex'));

        if (old) {
            throw new Error('dup table target')
        } else {
            this.tables.set(target.toString('hex'), table);
        }
    }

    use(middleware) {
        middleware(this);
    }
}


module.exports = RPC;

function validateId(id) {
    return id && id.length === 20;
}

function sha1(buf) {
    return Buffer.from(simpleSha1.sync(buf), 'hex');
}