const chai = require('chai');
const expect = chai.expect;

const describe = global.describe;
const it = global.it;
const before = global.before;
const after = global.after;
const beforeEach = global.beforeEach;
const afterEach = global.afterEach;

require('source-map-support').install();

let notif;
const init_notif = () => { notif = { notice: [], error: [], notif: new Map() }; };
init_notif();
const test_notif = () => {
	expect(notif.notice.length).to.equal(0);
	expect(notif.error.length).to.equal(0);
	expect(notif.notif.size).to.equal(0);
};

const config = {
	on_error: v => notif.error.push(v),
	on_notice: v => notif.notice.push(v)
};

const on_notif = ({ channel, payload }) => {
	const list = notif.notif.get(channel) || [];
	if (!list.length) {
		notif.notif.set(channel, list);
	}
	list.push(payload);
};

const derteberse = require('../').default;

/* eslint-disable require-await */

describe('Database', () => {

	describe('Connection', () => {

		let db;

		beforeEach(() => { db = new derteberse.DB(config); });
		afterEach(async () => { await db.close(); db = null; });

		beforeEach(() => init_notif());
		afterEach(() => test_notif());

		it('should create connection pool', async () => null);

		it('should create/acquire/release connection', async () => {
			const sym = Symbol();
			expect(await db.use(async () => sym)).to.equal(sym);
		});

		it('enable validation for next tests', () => { config.validate = true; });

	});

	describe('Querying', () => {
		let db;

		beforeEach(() => { db = new derteberse.DB(config); });
		afterEach(async () => { await db.close(); db = null; });

		beforeEach(() => init_notif());
		afterEach(() => test_notif());

		it('should execute query', async () => {
			const res = await db.query('select true as value;');
			expect(res.rows[0].value).to.equal(true);
		});

		it('should execute escaped query', async () => {
			const res = await db.query('select %s as %s;', '1', 'value');
			expect(res.rows[0].value).to.equal(1);
		});

		it('should throw on failed query', async () => {
			try {
				await db.transaction(async query => await query('select invalid from "also invalid"'));
				expect(true).to.equal(false);
			} catch (err) {
				expect(err.message).to.equal('relation "also invalid" does not exist');
			}
			init_notif();
		});

	});

	describe('Transaction', () => {
		let db;

		beforeEach(() => { db = new derteberse.DB(config); });
		afterEach(async () => { await db.close(); db = null; });

		beforeEach(() => init_notif());
		afterEach(() => test_notif());

		it('should execute escaped query in transaction', async () => {
			const res = await db.transaction(async query => await query('select %s as %s;', '1', 'value'));
			expect(res.rows[0].value).to.equal(1);
		});

	});

	describe('Event handlers', () => {
		let db;

		beforeEach(() => { db = new derteberse.DB(config); });
		afterEach(async () => { await db.close(); db = null; });

		beforeEach(() => init_notif());
		afterEach(() => test_notif());

		it('should receive "notice" event', async () => {
			await db.transaction(async query => await query('do $$ begin raise notice %L; end; $$;', 'potato'));
			expect(notif.notice.length).to.equal(1);
			expect(notif.error.length).to.equal(0);
			expect(notif.notice[0].message).to.equal('potato');
			init_notif();
		});

		it('should receive "error" event', async () => {
			try {
				await db.transaction(async query => await query('do $$ begin raise exception %L; end; $$;', 'potato'));
			} catch (err) {
				expect(err.message).to.equal('potato');
			}
			expect(notif.notice.length).to.equal(0);
			expect(notif.error.length).to.equal(1);
			expect(notif.error[0].message).to.equal('potato');
			init_notif();
		});

	});

});

describe('Asynchronous notifications', () => {

	beforeEach(() => init_notif());
	afterEach(() => test_notif());

	describe('Listener', () => {

		const opts = {
			poll_interval: 0,
			json: false,
			on_error: config.on_error
		};

		let db;
		let listener;

		const ping = async () => {
			await new Promise(res => setTimeout(res, 10));
			await db.query('select true;');
		};

		const sleep = async () => {
			await new Promise(res => setTimeout(res, 20));
		};

		before(() => { db = new derteberse.DB(config); listener = new derteberse.Listener(db, opts); });
		after(async () => { await listener.close(); await db.close(); db = null; });

		it('should initialise', async () => await listener.init());

		it('should subscribe then receive', async () => {
			await listener.subscribe('potato', on_notif);
			await db.query('notify %I, %L', 'potato', 'lemon');
			await ping();
			expect(notif.notif.has('potato')).to.equal(true);
			expect(notif.notif.get('potato').length).to.equal(1);
			expect(notif.notif.get('potato')[0]).to.equal('lemon');
			notif.notif.delete('potato');
		});

		it('should unsubscribe then not receive', async () => {
			await listener.unsubscribe('potato', on_notif);
			await db.query('notify %I, %L', 'potato', 'lemon');
			await ping();
		});

		it('should subscribe then receive', async () => {
			await listener.subscribe('potato', on_notif);
			await db.query('notify %I, %L', 'potato', 'lemon');
			await ping();
			expect(notif.notif.has('potato')).to.equal(true);
			expect(notif.notif.get('potato').length).to.equal(1);
			expect(notif.notif.get('potato')[0]).to.equal('lemon');
			notif.notif.delete('potato');
		});

		it('should reset then not receive', async () => {
			await listener.reset();
			await db.query('notify %I, %L', 'potato', 'lemon');
			await ping();
		});

		it('should not receive from different channel', async () => {
			await listener.subscribe('potato', on_notif);
			await db.query('notify %I, %L', 'tomato', 'lemon');
			await ping();
			expect(notif.notif.has('potato')).to.equal(false);
			expect(notif.notif.has('tomato')).to.equal(false);
		});

		it('should receive automatically when polling enabled', async () => {
			await listener.subscribe('potato', on_notif);
			await listener.poll_start(10);
			await db.query('notify %I, %L', 'potato', 'lemon');
			await sleep();
			expect(notif.notif.has('potato')).to.equal(true);
			expect(notif.notif.get('potato').length).to.equal(1);
			expect(notif.notif.get('potato')[0]).to.equal('lemon');
			notif.notif.delete('potato');
			await listener.unsubscribe('potato', on_notif);
		});

		it('should not receive automatically with polling disabled', async () => {
			await listener.subscribe('potato', on_notif);
			await listener.poll_start(10);
			await db.query('notify %I, %L', 'potato', 'lemon');
			await sleep();
			expect(notif.notif.has('potato')).to.equal(true);
			expect(notif.notif.get('potato').length).to.equal(1);
			expect(notif.notif.get('potato')[0]).to.equal('lemon');
			notif.notif.delete('potato');
		});

	});

	describe('Polling', () => {

		const opts = {
			poll_interval: 0,
			json: false,
			on_error: config.on_error
		};

		let db;
		let listener;

		const sleep = async () => await new Promise(res => setTimeout(res, 2 * 10));

		before(() => { db = new derteberse.DB(config); listener = new derteberse.Listener(db, opts); });
		after(async () => { await listener.close(); await db.close(); db = null; });

		beforeEach(async () => await listener.subscribe('potato', on_notif));
		afterEach(async () => await listener.unsubscribe('potato', on_notif));

		it('should receive automatically when polling enabled', async () => {
			await listener.poll_start(10);
			await db.query('notify %I, %L', 'potato', 'lemon');
			await sleep();
			expect(notif.notif.has('potato')).to.equal(true);
			expect(notif.notif.get('potato').length).to.equal(1);
			expect(notif.notif.get('potato')[0]).to.equal('lemon');
			notif.notif.delete('potato');
			await listener.poll_stop();
		});

		/* Some update to node-pg causes async notifications to be delivered poll-free now */
//		it('should not receive automatically with polling disabled', async () => {
//			await db.query('notify %I, %L', 'potato', 'lemon');
//			await sleep();
//			expect(notif.notif.has('potato')).to.equal(false);
//		});

	});

	describe('JSON payloads', () => {

		const opts = {
			poll_interval: 0,
			json: true,
			on_error: config.on_error
		};

		let db;
		let listener;

		const ping = async () => {
			await new Promise(res => setTimeout(res, 10));
			await db.query('select true;');
		};

		before(() => { db = new derteberse.DB(config); listener = new derteberse.Listener(db, opts); });
		after(async () => { await listener.close(); await db.close(); db = null; });


		beforeEach(async () => await listener.subscribe('potato', on_notif));
		afterEach(async () => await listener.unsubscribe('potato', on_notif));

		it('should successfully decode valid JSON', async () => {
			const json = JSON.stringify({ name: ['value', 2, false, null] });
			await db.query('notify %I, %L', 'potato', json);
			await ping();
			expect(notif.notif.has('potato')).to.equal(true);
			expect(notif.notif.get('potato').length).to.equal(1);
			expect(JSON.stringify(notif.notif.get('potato')[0])).to.equal(json);
			notif.notif.delete('potato');
		});

		it('should fail to decode invalid JSON', async () => {
			const json = '{ not_valid_json: true }';
			await db.query('notify %I, %L', 'potato', json);
			await ping();
			expect(notif.error.length).to.equal(1);
			expect(/Invalid JSON/.test(notif.error[0].message)).to.equal(true);
			notif.error.length = 0;
		});
	});

});
