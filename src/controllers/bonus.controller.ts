import { NextFunction, Request, Response } from 'express';

import { bonusQueue } from '../queue';
import { spendBonus } from '../services/bonus.service';

type AppError = Error & { status?: number };

function createAppError(message: string, status: number): AppError {
  const error = new Error(message) as AppError;
  error.status = status;
  return error;
}

export async function spendUserBonus(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  try {
    const amount = Number(req.body?.amount);

    if (!Number.isInteger(amount) || amount <= 0) {
      throw createAppError('amount must be a positive integer', 400);
    }

    const idempotencyKey = req.headers['idempotency-key'];
    const requestId =
      (typeof idempotencyKey === 'string' ? idempotencyKey : undefined) ||
      req.body?.requestId;

    if (!requestId || typeof requestId !== 'string') {
      throw createAppError('requestId is required (body.requestId or Idempotency-Key header)', 400);
    }

    const result = await spendBonus(req.params.id, amount, requestId);

    res.json(result);
  } catch (error) {
    next(error);
  }
}

export async function enqueueExpireAccrualsJob(
  _req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  try {
    await bonusQueue.add(
      'expireAccruals',
      { createdAt: new Date().toISOString() },
      {
        jobId: 'expire-accruals',
        attempts: 3,
        backoff: { type: 'exponential', delay: 1000 },
        removeOnComplete: true,
      },
    );

    res.json({ queued: true });
  } catch (error) {
    next(error);
  }
}
