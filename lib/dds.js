const LRU = require('lru');
const randombytes = require('randombytes');
const simpleSha1 = require('simple-sha1');
const records = require('record-cache');
const low = require('last-one-wins');

const RPC = require('../lib/rpc');



class DDS extends RPC {
	constructor(opts = {}) {
		super(opts);

	}
}



module.exports.DDS = DDS;


function sha1(buf) {
	return Buffer.from(simpleSha1.sync(buf), 'hex')
}