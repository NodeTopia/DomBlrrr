const RPC = require('../lib/rpc');
const Table = require('../lib/table');

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


let table1 = new Table({
    target: 'test',
    rpc: rpc1
});
let table2 = new Table({
    target: 'test',
    rpc: rpc2
});
rpc1.addTable(table1)
rpc2.addTable(table2)

rpc1.on('node', onNode);

rpc1.on('ready', async function () {
    await table1.boostrap();
    console.log('table1 boostrap complete')
    await table2.boostrap();
    console.log('table2 boostrap complete')
});

rpc2.on('ready', async function () {
    rpc1.socket.bind(8080);
});
rpc2.socket.bind(8081);