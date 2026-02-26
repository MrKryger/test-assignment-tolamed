import {Transaction} from 'sequelize';

import {sequelize} from '../db';
import {BonusTransaction} from '../models/BonusTransaction';

type AppError = Error & { status?: number };

function createAppError(message: string, status: number): AppError {
    const error = new Error(message) as AppError;
    error.status = status;
    return error;
}

async function lockUserTransactions(
    userId: string,
    transaction: Transaction,
): Promise<BonusTransaction[]> {
    return BonusTransaction.findAll({
        where: {user_id: userId},
        transaction,
        lock: transaction.LOCK.UPDATE,
    });
}

function findExistingByRequestId(
    rows: BonusTransaction[],
    requestId: string,
): BonusTransaction | undefined {
    return rows.find((tx) => tx.request_id === requestId);
}

function handleDuplicate(
    existing: BonusTransaction,
    amount: number,
): SpendResult {
    if (existing.amount === amount && existing.type === 'spend') {
        return {success: true, duplicated: true};
    }
    throw createAppError(
        'Conflict: requestId already used with different payload',
        409,
    );
}

function calculateBalance(rows: BonusTransaction[]): number {
    const now = new Date();

    const accrualTotal = rows
        .filter(
            (tx) =>
                tx.type === 'accrual' &&
                (tx.expires_at === null || tx.expires_at > now),
        )
        .reduce((sum, tx) => sum + tx.amount, 0);

    const spendTotal = rows
        .filter((tx) => tx.type === 'spend')
        .reduce((sum, tx) => sum + tx.amount, 0);

    return accrualTotal - spendTotal;
}

export interface SpendResult {
    success: boolean;
    duplicated: boolean;
}

export async function spendBonus(
    userId: string,
    amount: number,
    requestId: string,
): Promise<SpendResult> {
    return sequelize.transaction(async (transaction) => {
        const rows = await lockUserTransactions(userId, transaction);
        const existing = findExistingByRequestId(rows, requestId);
        if (existing) {
            return handleDuplicate(existing, amount);
        }
        const balance = calculateBalance(rows);
        if (balance < amount) {
            throw createAppError('Not enough bonus', 400);
        }
        await BonusTransaction.create(
            {
                user_id: userId,
                type: 'spend',
                amount,
                expires_at: null,
                request_id: requestId,
            },
            {transaction},
        );
        return {success: true, duplicated: false};
    });
}
