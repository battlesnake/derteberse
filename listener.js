const _ = require('lodash');
const escape = require('pg-escape');
module.exports = Listener;

const default_opts = {
	poll_interval: 10,
	json: true
};

function Listener(derteberse, opts) {

	const { poll_interval, json } = _.defaults({}, opts, default_opts);

	const con = derteberse.steal();

	/* Callback table */
	const subs = new Map();

	/* Dispatch notifications */
	con.then(client => {
		client.on('notification', ({ channel, payload }) => {
			const cb = subs.get(channel);
			if (!cb) {
				return;
			}
			if (json) {
				try {
					payload = JSON.parse(payload);
				} catch (err) {
					console.error(`Failed to process notification payload on channel "${channel}": Invalid JSON`);
					return;
				}
			}
			cb.forEach(func => process.nextTick(() => func({ channel, payload })));
		});
		/* Ping for notifications */
		const resetTimer = () => setTimeout(() => client.query('select true;', resetTimer), poll_interval);
		resetTimer();
	});

	this.subscribe = (topic, func) => new Promise((res, rej) => {
		const cb = subs.get(topic) || new Set();
		const is_new = cb.size === 0;
		if (is_new) {
			subs.set(topic, cb);
			con.then(client => client.query(escape('listen %I;', topic), err => err ? rej(err) : res()));
		}
		cb.add(func);
		if (!is_new) {
			res();
		}
	});

	this.unsubscribe = (topic, func) => new Promise((res, rej) => {
		const cb = subs.get(topic);
		if (!cb || !cb.delete(func)) {
			return;
		}
		if (cb.size === 0) {
			subs.delete(topic);
			con.then(client => client.query(escape('unlisten %I;', topic), err => err ? rej(err) : res()));
		} else {
			res();
		}
	});

}
