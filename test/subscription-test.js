const RPC = require('../lib/rpc');
const Subscription = require('../lib/subscription');

function onNode(node) {
    console.log(`new node ${node.id.toString('hex')} ${node.host}:${node.port} count:${this.nodes.count()}`)

}

let rpc1 = new RPC({
    bootstrap: ['127.0.0.1:8081'],
    host: '127.0.0.1',
    port: 8080
});

let rpc2 = new RPC({
    host: '127.0.0.1',
    port: 8081
});


let sub1 = new Subscription({
    target: 'test',
    rpc: rpc1
});
let sub2 = new Subscription({
    target: 'test',
    rpc: rpc2
});
rpc1.addTable(sub1)
rpc2.addTable(sub2)

rpc1.on('node', onNode);

rpc1.on('ready', async function () {
    await sub1.bootstrap();
    console.log('table1 boostrap complete')



    await sub2.bootstrap();
    console.log('table2 boostrap complete')

    sub1.subscribe(function (data) {
        console.log(`sub1.subscribe`, data);
    })

    sub1.publish({ test: true })
});

rpc2.on('ready', async function () {
    rpc1.socket.bind(8080);
});
rpc2.socket.bind(8081);