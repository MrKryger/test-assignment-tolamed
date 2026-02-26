import {Queue, Worker} from 'bullmq';
import {Op} from 'sequelize';

import {sequelize} from './db';
import {BonusTransaction} from './models/BonusTransaction';
import {redis} from './redis';

const queueConnection = redis.duplicate();

export const bonusQueue = new Queue('bonusQueue', {
    connection: queueConnection,
});

let expireAccrualsWorker: Worker | null = null;

export function startExpireAccrualsWorker(): Worker {
    if (expireAccrualsWorker) {
        return expireAccrualsWorker;
    }

    expireAccrualsWorker = new Worker(
        'bonusQueue',
        async (job) => {
            if (job.name === 'expireAccruals') {
                console.log(`[worker] expireAccruals started, jobId=${job.id}`);
                await processExpiredAccruals();
                console.log(`[worker] expireAccruals completed, jobId=${job.id}`);
            }
        },
        {
            connection: redis.duplicate(),
        },
    );

    expireAccrualsWorker.on('failed', (job, err) => {
        console.error(`[worker] failed, jobId=${job?.id}`, err);
    });

    return expireAccrualsWorker;
}

async function processExpiredAccruals(): Promise<void> {
    const expiredAccruals = await BonusTransaction.findAll({
        where: {
            type: 'accrual',
            expires_at: {[Op.lt]: new Date()},
        },
    });

    for (const accrual of expiredAccruals) {
        const requestId = `expire:${accrual.id}`;

        await sequelize.transaction(async (transaction) => {
            const existing = await BonusTransaction.findOne({
                where: {
                    user_id: accrual.user_id,
                    request_id: requestId,
                },
                transaction,
            });

            if (existing) {
                console.log(`[worker] skip already-expired accrual ${accrual.id}`);
                return;
            }

            await BonusTransaction.create(
                {
                    user_id: accrual.user_id,
                    type: 'spend',
                    amount: accrual.amount,
                    expires_at: null,
                    request_id: requestId,
                },
                {transaction},
            );

            console.log(`[worker] expired accrual ${accrual.id}, amount=${accrual.amount}`);
        });
    }
}
