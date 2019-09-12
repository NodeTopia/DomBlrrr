
const simpleSha1 = require('simple-sha1');
const EventEmitter = require('events').EventEmitter;
const KBucket = require('k-bucket');


class Table extends EventEmitter {
    constructor({ query, target, rpc }) {
        super();


        this.targetName = target;
        this.target = sha1(target);
        this.targetString = this.target.toString('hex');

        this.bootstrapped = false;
        this.bootstrapping = false;

        this.methods = [];


        this.rpc = rpc;

        this.table = new KBucket({
            localNodeId: this.target
        });


    }

    addMethod(method, func, scope) {
        this.methods.push([method, func, scope]);
    }

    addNode(node) {
        this.table.add(node);
    }

    bootstrap() {

        if (this.bootstrapped) {

            if (this.table.toArray().length > 1) {
                return Promise.resolve();
            } else if (this.table.toArray().length == 1 && this.bootstrapping) {
                return Promise.resolve();
            }
        }

        const self = this;
        let queried = {};


        return new Promise(function (resolve, reject) {

            self.bootstrapping = true;
            self.bootstrapped = true;

            function done(err, n) {
                if (err) console.log(err);
                self.bootstrapping = false;
                resolve();
            }

            function onreply(message, node) {
                if (!message.nodes)
                    return true;

                let id = node.host + ':' + node.port;

                if (queried[id])
                    return;

                queried[id] = true;

                message.peers.forEach(function (node) {
                    self.addNode(node);
                });

            }

            self.rpc.closest('_find_peers', {
                target: self.target
            }, onreply).then(done);
        });
    }
}

function sha1(buf) {
    return Buffer.from(simpleSha1.sync(buf), 'hex')
}

module.exports = Table;