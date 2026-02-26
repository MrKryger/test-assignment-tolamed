import {randomUUID} from 'crypto';
import {Op} from 'sequelize';
import request from 'supertest';
import {QueueEvents} from 'bullmq';

import app from '../src/app';
import {sequelize} from '../src/db';
import {BonusTransaction} from '../src/models/BonusTransaction';
import {User} from '../src/models/User';
import {bonusQueue, startExpireAccrualsWorker} from '../src/queue';
import {redis} from '../src/redis';

async function createTestUser(
    name: string,
    accruals: Array<{ amount: number; expiresAt?: Date | null }> = [],
): Promise<string> {
    const user = await User.create({name});
    for (const {amount, expiresAt} of accruals) {
        await BonusTransaction.create({
            user_id: user.id,
            type: 'accrual',
            amount,
            expires_at: expiresAt ?? null,
            request_id: null,
        });
    }
    return user.id;
}

beforeAll(async () => {
    await sequelize.authenticate();
});

afterAll(async () => {
    await bonusQueue.close();
    await sequelize.close();
    await redis.quit();
});

describe('Idempotency: duplicate requestId does not create a second spend', () => {
    it('returns duplicated: true on repeat and creates only one spend', async () => {
        const userId = await createTestUser('idem-user', [{amount: 500}]);
        const requestId = randomUUID();

        const res1 = await request(app)
            .post(`/users/${userId}/spend`)
            .send({amount: 100, requestId});

        expect(res1.status).toBe(200);
        expect(res1.body).toEqual({success: true, duplicated: false});

        const res2 = await request(app)
            .post(`/users/${userId}/spend`)
            .send({amount: 100, requestId});

        expect(res2.status).toBe(200);
        expect(res2.body).toEqual({success: true, duplicated: true});

        const spends = await BonusTransaction.findAll({
            where: {user_id: userId, type: 'spend'},
        });
        expect(spends.length).toBe(1);
    });

    it('returns 409 when same requestId used with different amount', async () => {
        const userId = await createTestUser('idem-conflict', [{amount: 500}]);
        const requestId = randomUUID();

        await request(app)
            .post(`/users/${userId}/spend`)
            .send({amount: 100, requestId});

        const res = await request(app)
            .post(`/users/${userId}/spend`)
            .send({amount: 200, requestId});

        expect(res.status).toBe(409);
    });

    it('uses Idempotency-Key header over body.requestId', async () => {
        const userId = await createTestUser('idem-header', [{amount: 500}]);
        const headerKey = randomUUID();
        const bodyKey = randomUUID();

        const res1 = await request(app)
            .post(`/users/${userId}/spend`)
            .set('Idempotency-Key', headerKey)
            .send({amount: 100, requestId: bodyKey});

        expect(res1.status).toBe(200);
        expect(res1.body.duplicated).toBe(false);

        const res2 = await request(app)
            .post(`/users/${userId}/spend`)
            .set('Idempotency-Key', headerKey)
            .send({amount: 100, requestId: bodyKey});

        expect(res2.status).toBe(200);
        expect(res2.body.duplicated).toBe(true);
    });
});

describe('Expired accruals are excluded from balance', () => {
    it('rejects spend when only expired accruals exist', async () => {
        const pastDate = new Date(Date.now() - 86400_000);
        const userId = await createTestUser('expired-only', [
            {amount: 500, expiresAt: pastDate},
        ]);

        const res = await request(app)
            .post(`/users/${userId}/spend`)
            .send({amount: 1, requestId: randomUUID()});

        expect(res.status).toBe(400);
        expect(res.body.message).toMatch(/not enough bonus/i);
    });

    it('only counts non-expired accruals in balance', async () => {
        const pastDate = new Date(Date.now() - 86400_000);
        const futureDate = new Date(Date.now() + 86400_000 * 30);
        const userId = await createTestUser('mixed-expiry', [
            {amount: 300, expiresAt: futureDate},
            {amount: 200, expiresAt: pastDate},
        ]);

        const res1 = await request(app)
            .post(`/users/${userId}/spend`)
            .send({amount: 300, requestId: randomUUID()});

        expect(res1.status).toBe(200);

        const res2 = await request(app)
            .post(`/users/${userId}/spend`)
            .send({amount: 1, requestId: randomUUID()});

        expect(res2.status).toBe(400);
    });
});

describe('Concurrent spends do not overdraw balance', () => {
    it('only one of concurrent spends succeeds when balance allows only one', async () => {
        const userId = await createTestUser('concurrent-one', [{amount: 100}]);

        const promises = Array.from({length: 5}, () =>
            request(app)
                .post(`/users/${userId}/spend`)
                .send({amount: 100, requestId: randomUUID()}),
        );

        const results = await Promise.all(promises);

        const successes = results.filter((r) => r.status === 200);
        const failures = results.filter((r) => r.status !== 200);

        expect(successes.length).toBe(1);
        expect(failures.length).toBe(4);

        const spends = await BonusTransaction.findAll({
            where: {user_id: userId, type: 'spend'},
        });
        expect(spends.length).toBe(1);
    });

    it('partial concurrent spends respect total balance', async () => {
        const userId = await createTestUser('concurrent-partial', [{amount: 100}]);

        const promises = Array.from({length: 3}, () =>
            request(app)
                .post(`/users/${userId}/spend`)
                .send({amount: 50, requestId: randomUUID()}),
        );

        const results = await Promise.all(promises);

        const successes = results.filter((r) => r.status === 200);

        expect(successes.length).toBe(2);

        const totalSpent = (
            await BonusTransaction.findAll({
                where: {user_id: userId, type: 'spend'},
            })
        ).reduce((s, tx) => s + tx.amount, 0);

        expect(totalSpent).toBe(100);
    });
});

describe('Expire-accruals queue idempotency', () => {
    it('creates spend transactions for expired accruals and does not duplicate on re-run', async () => {
        const pastDate = new Date(Date.now() - 86400_000);
        const userId = await createTestUser('queue-expire', [
            {amount: 200, expiresAt: pastDate},
            {amount: 300, expiresAt: pastDate},
        ]);

        const worker = startExpireAccrualsWorker();
        const queueEvents = new QueueEvents('bonusQueue', {
            connection: {host: redis.options.host, port: redis.options.port},
        });

        try {
            // First run
            const job1 = await bonusQueue.add(
                'expireAccruals',
                {createdAt: new Date().toISOString()},
                {
                    jobId: `test-expire-${randomUUID()}`,
                    attempts: 3,
                    backoff: {type: 'exponential', delay: 1000},
                },
            );
            await job1.waitUntilFinished(queueEvents, 10000);

            const spendsAfterFirst = await BonusTransaction.findAll({
                where: {
                    user_id: userId,
                    type: 'spend',
                    request_id: {[Op.like]: 'expire:%'},
                },
            });
            expect(spendsAfterFirst.length).toBe(2);

            // Second run — should NOT create duplicates
            const job2 = await bonusQueue.add(
                'expireAccruals',
                {createdAt: new Date().toISOString()},
                {
                    jobId: `test-expire-${randomUUID()}`,
                    attempts: 3,
                    backoff: {type: 'exponential', delay: 1000},
                },
            );
            await job2.waitUntilFinished(queueEvents, 10000);

            const spendsAfterSecond = await BonusTransaction.findAll({
                where: {
                    user_id: userId,
                    type: 'spend',
                    request_id: {[Op.like]: 'expire:%'},
                },
            });
            expect(spendsAfterSecond.length).toBe(2);
        } finally {
            await queueEvents.close();
            await worker.close();
        }
    }, 30000);
});

describe('Input validation', () => {
    it('returns 400 for missing requestId', async () => {
        const userId = await createTestUser('val-missing', [{amount: 100}]);
        const res = await request(app)
            .post(`/users/${userId}/spend`)
            .send({amount: 50});

        expect(res.status).toBe(400);
    });

    it('returns 400 for invalid amount', async () => {
        const userId = await createTestUser('val-amount', [{amount: 100}]);
        const res = await request(app)
            .post(`/users/${userId}/spend`)
            .send({amount: -10, requestId: randomUUID()});

        expect(res.status).toBe(400);
    });
});
