import _ from 'lodash';
import escape from 'pg-escape';
import EventEmitter from 'eventemitter';

export const default_opts = {
	poll_interval: 0,
	json: true,
	on_error: null
};

export default class Listener extends EventEmitter {

	constructor(derteberse, opts) {
		super();
		this.config = _.defaults({}, opts, default_opts);
		this.db = derteberse;
		/* Callback table */
		this.subs = new Map();
	}

	async init() {
		/* Will be called lazily if not called explicitly by consumer */
		if (this.initialised) {
			return;
		}

		/* Steal connection */
		this.client = await this.db.steal();

		/* Dispatch notifications */
		this.client.on('notification', (...args) => this.inject(...args));

		this.initialised = true;

		if (this.config.on_error) {
			this.on('error', this.config.on_error);
		}

		if (this.config.poll_interval) {
			await this.poll_start();
		}
	}

	inject({ channel, payload }) {
		const cb = this.subs.get(channel);
		/* Potential race condition when unsubscribing */
		// istanbul ignore next
		if (!cb) {
			return;
		}
		if (this.config.json) {
			try {
				payload = JSON.parse(payload);
			} catch (err) {
				this.emit('error', new Error(`Failed to process notification payload on channel "${channel}": Invalid JSON`));
				return;
			}
		}
		this.emit('notification', { channel, payload });
		this.emit(`+${channel}`, { channel, payload });
		cb.forEach(func => process.nextTick(() => func({ channel, payload })));
	}

	async poll_start(interval = 0) {
		if (this.poll_timer) {
			await this.poll_stop();
		}
		await this.poll();
		this.poll_timer = setInterval(async () => {
			try {
				await this.poll();
			} catch (err) {
				// istanbul ignore next
				this.poll_stop();
				// istanbul ignore next
				throw new Error(`Failed to poll for database notifications: ${err.message}`);
			}
		}, interval || this.config.poll_interval);
	}

	async poll_stop() { // eslint-disable-line require-await
		if (!this.poll_timer) {
			return;
		}
		clearInterval(this.poll_timer);
		this.poll_timer = null;
	}

	async poll() {
		await this.init();
		await this.client.query('select true;');
	}

	async subscribe(channel, func) {
		if (!func) {
			throw new Error('Subscribe called without function to subscribe');
		}
		await this.init();
		const cb = this.subs.get(channel) || new Set();
		const is_new = cb.size === 0;
		cb.add(func);
		if (!is_new) {
			return;
		}
		this.subs.set(channel, cb);
		try {
			await this.client.query(escape('listen %I;', channel));
		} catch (err) {
			// istanbul ignore next
			this.subs.unset(channel, cb);
			// istanbul ignore next
			throw err;
		}
	}

	async unsubscribe(channel, func) {
		if (!func) {
			throw new Error('Unsubscribe called without function to unsubscribe');
		}
		await this.init();
		const cb = this.subs.get(channel);
		if (!cb || !cb.delete(func)) {
			return;
		}
		if (cb.size === 0) {
			this.subs.delete(channel);
			await this.client.query(escape('unlisten %I;', channel));
		}
	}

	async reset() {
		await this.init();
		const topics = [...this.subs.keys()];
		this.subs.clear();
		await Promise.all(topics.map(async channel => await this.client.query(escape('unlisten %I;', channel))));
	}

	async close() {
		if (!this.initialised) {
			return;
		}
		await this.poll_stop();
		await this.client.close();
	}

}
