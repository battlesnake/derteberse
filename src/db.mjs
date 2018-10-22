import _ from 'lodash';
import postgres from 'pg';
import generic_pool from 'generic-pool';
import pg_escape from 'pg-escape';
import EventEmitter from 'eventemitter';

/* istanbul ignore next */
export const default_config = {
	db: {
		host: 'localhost',
		port: 5432
	},
	pool: {
		max: 20,
		log: false,
		refreshIdle: false
	},
	native: false,
	format: (template, args) => pg_escape(template, ...args),
	on_error: err => null, // eslint-disable-line no-unused-vars, handle-callback-err
	on_notice: notice => null, // eslint-disable-line no-unused-vars
};

export default class DB extends EventEmitter {

	constructor(configuration) {
		super();

		const config = _.defaultsDeep(configuration, default_config);

		/* istanbul ignore next */
		const pg = config.native ? postgres.native : postgres;

		const create = async () => {
			const client = new pg.Client(config.db);
			client.on('error', err => this.emit('error', err));
			client.on('notice', notice => this.emit('notice', notice));
			if (config.on_error) {
				this.on('query-error', config.on_error);
			}
			if (config.on_notice) {
				this.on('notice', config.on_notice);
			}
			/* Monkeypatch */
			const _query = client.query.bind(client);
			client.query = async (template, ...args) => {
				try {
					if (config.format) {
						return await _query(config.format(template, args));
					} else {
						if (args.length) {
							throw new Error('Template arguments specified but no formatter has been configured');
						}
						return await client._query(template);
					}
				} catch (err) {
					this.emit('query-error', err);
					throw err;
				}
			};
			await client.connect();
			return client;
		};

		const validate = async client => {
			try {
				await client.query('select true;');
				return true;
			} catch (err) {
				return false;
			}
		};

		const destroy = async client => await client.end();

		const pool_config = _.defaults(
			config.pool,
			{ name: `derteberse(${config.db.user}@${config.db.database})` });

		const factory = { create, destroy };

		if (config.validate) {
			factory.validate = validate;
		}

		/* Create pool */
		this.pool = generic_pool.createPool(factory, pool_config);
	}

	/* Provide client to callback (which returns promise) */
	async use(content) {
		const client = await this.pool.acquire();
		try {
			const res = await content(client);
			await this.pool.release(client);
			return res;
		} catch (err) {
			await this.pool.destroy(client);
			throw err;
		}
	}

	/* Run callback (which returns promise) inside a transaction */
	async transaction(content) {
		return await this.use(async client => {
			try {
				await client.query('BEGIN TRANSACTION');
				const res = await content(client.query);
				await client.query('COMMIT');
				return res;
			} catch (err) {
				await client.query('ROLLBACK');
				throw err;
			}
		});
	}

	/* Run query */
	async query(template, ...args) {
		const client = await this.pool.acquire();
		try {
			const res = await client.query(template, ...args);
			await this.pool.release(client);
			return res;
		} catch (err) {
			await this.pool.destroy(client);
			throw err;
		}
	}

	/* Steal a connection from the pool */
	async steal() {
		const client = await this.pool.acquire();
		client.close = async () => await this.pool.destroy(client);
		return client;
	}

	/* Close database connections */
	async close() {
		await this.pool.drain();
		await this.pool.clear();
		delete this.pool;
	}

}
