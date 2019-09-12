const express = require('express');
const isIP = require('net').isIP;
const dns = require('dns');
const util = require('util');
const events = require('events');
const uuid = require('node-uuid');
const BJSON = require('buffer-json');
const http = require('http');

const ETIMEDOUT = new Error('Query timed out');
ETIMEDOUT.code = 'ETIMEDOUT';

const EUNEXPECTEDNODE = new Error('Unexpected node id');
EUNEXPECTEDNODE.code = 'EUNEXPECTEDNODE';


class HTTP extends events.EventEmitter {
	constructor(opts) {

		super();

		this.timeout = opts.timeout || 2000;
		this.inflight = 0;
		this.destroyed = false;
		this.isIP = opts.isIP || isIP;

		this.port = opts.port || 8080;
		this.host = opts.host;

		this.id = opts.id;

		this.inflight = 0;

		this.onQuery = opts.query;

		this.router = express.Router();

		this.router.post('/dht', this.onRequest.bind(this))

		this.connections = [];


	}

	send(message, node) {
		let self = this;
		this.inflight++;
		return new Promise(function (resolve, reject) {
			const options = {
				hostname: node.host,
				port: node.port,
				path: '/dht',
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				}
			};

			const req = http.request(options, async function (res) {
				//console.log(`statusCode: ${res.statusCode}`);
				self.inflight--;
				let [error, message, rinfo] = await self.parseStream(res);
				if (error) {
					resolve([error, message, rinfo]);
				} else {
					resolve([error, message, rinfo]);
				}
			});

			req.on('error', function (error) {
				self.inflight--;
				resolve(error);
			});

			self.stringifyStream(req, message);

			req.end();
		});
	}

	async onRequest(req, res) {
		let self = this;
		let [error, message, rinfo] = await this.parseStream(req);

		this.onQuery(message, rinfo).then(function (responce) {

			self.stringifyStream(res, responce);

			res.end();
		}).catch(function (err) {

			self.stringifyStream(res, null, err);

			res.end();
		});

	}

	stringifyStream(stream, message, error) {

		let data = {
			message: message,
			error: error,
			rinfo: {
				host: this.host,
				port: this.port,
				id: this.id
			}
		};
		var buffer = BJSON.stringify(data);

		stream.write(buffer);

	}

	parseStream(stream) {
		return new Promise(function (resolve, reject) {

			let data = [];
			stream.on('data', function (c) {
				data.push(c.toString());
				//console.log(c.toString())
			}).on('end', function () {

				let parse;
				try {
					parse = BJSON.parse(data.join(''));
				} catch (e) {
					return self.emit('warning', e);
				}
				resolve([parse.error, parse.message, parse.rinfo]);
			});
		});

	}
	onListening() {
		this.emit('listening');
	}
	bind(...args) {
		if (this.server)
			return;

		this.server = express();

		this.server.use(this.router);

		this.server.on('listening', this.onListening.bind(this));
		if (arguments.length == 0) {
			this.server.listen(this.host, this.port)
		} else {
			args.push(this.onListening.bind(this))
			this.server.listen.apply(this.server, args);
		}
	}

	destroy(cb) {
		this.destroyed = true;
		this.server.close()
	}
}
module.exports = HTTP