const q = require('q');
const _ = require('lodash');
const postgres = require('pg');
const Pool = require('generic-pool').Pool;

const defaultConfig = {
	db: {
		host: 'localhost',
		port: 5432
	},
	pool: {
		max: 20,
		idleTimeoutMillis: 30000,
		log: false
	},
	native: false,
	on_notice: notice => null,
	on_error: err => console.log('PG', err)
};

module.exports = function (configuration) {

	const config = _.defaultsDeep(
		{ pool: {
			create: poolAdd,
			destroy: poolRemove,
		} },
		configuration,
		defaultConfig);

	config.pool.name = `derteberse(${config.db.user}@${config.db.database})`;

	const pool = new Pool(config.pool);

	const pg = config.native ? postgres.native : postgres;

	return { query, transaction };

	function poolAdd(cb) {
		const client = new pg.Client(config.db);
		client.connect(err => {
			if (err) {
				return cb(err);
			}
			client.on('error', err => {
				pool.destroy(client);
				if (config.on_error) {
					config.on_error(err);
				}
			});
			if (config.on_notice) {
				client.on('notice', config.on_notice);
			}
			client.runQuery = q.nbind(client.query, client);
			client.runQuery.client = client;
			cb(null, client);
		});
	}

	function poolRemove(client) {
		client.end();
	}

	function transaction(content) {
		return q.ninvoke(pool, 'acquire')
			.then(client => client.runQuery('BEGIN TRANSACTION'))
			.then(client => content(client.runQuery)
				.then(
					res => client.runQuery('COMMIT').thenResolve(res),
					err => client.runQuery('ROLLBACK').thenReject(err))
				.finally(() => pool.release(client)));
	}

	function query(sql) {
		return q.ninvoke(pool, 'acquire')
			.then(client => client.runQuery(sql)
				.finally(() => pool.release(client)));
	}

};
