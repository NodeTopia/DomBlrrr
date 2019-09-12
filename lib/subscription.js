
const simpleSha1 = require('simple-sha1');
const EventEmitter = require('events').EventEmitter;
const KBucket = require('k-bucket');
const Table = require('./table');


class Subscription extends Table {
    constructor(opts) {
        super(opts);
        this.addMethod('publish', this.onPublish, this);
        this.addMethod('unsubscribe', this.onUnsubscribe, this);
        this.addMethod('subscribe', this.onSubscribe, this);
    }

    async queryNode(method, data, node) {
        const self = this;
        let [err, result, rinfo] = await this.rpc.query(method, data, node);

        if (err) {
            this.emit('warning', err);
        }

        return [err, result, rinfo];
    }

    notify(action) {
        const self = this;

        const nodes = this.table.toArray();

        nodes.forEach(function (node) {

            self.queryNode(action, {
                id: self.rpc.id,
                target: self.target,
                targetName: self.targetName
            }, node);

        });
        return true;
    }

    distribute(data) {
        const self = this;

        const nodes = this.table.toArray();

        nodes.forEach(send);

        async function send(node) {

            if (node.id.toString('hex') != self.rpc.id.toString('hex')) {



                let [err, result, rinfo] = await self.queryNode('publish', {
                    id: self.rpc.id,
                    target: self.target,
                    targetName: self.targetName,
                    data: data
                }, node);

                if (err) {
                    if (rinfo.id)
                        self.table.remove(rinfo.id);

                    return console.log(err, result, rinfo)

                }

            }
        }
        return true;
    }

    onPublish(data, node) {
        if (this.role == 'broker') {
            this.distribute(data);
        }

        this.emit('event', data, node);

        return {};

    }
    onSubscribe(subscriber) {
        this.addSubscriber(subscriber);
        return {};
    }
    onUnsubscribe(subscriber) {
        this.table.remove(subscriber.id);
        return {};
    }
    async subscribe(func, cb) {
        this.on('event', func);
        if (this.listenerCount('event') == 1) {
            await this.bootstrap();
            await this.notify('subscribe');
        }
    }

    unSubscribe(func) {
        this.removeListener('event', func);

        if (this.listenerCount('event') == 0) {
            this.emit('remove');
            this.notify('unsubscribe');
        }
    }

    async publish(data) {
        const self = this;
        await this.bootstrap();
        this.emit('event', data);
        this.distribute(data);
    }
}
module.exports = Subscription;