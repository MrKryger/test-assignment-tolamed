import { NextFunction, Request, Response } from 'express';

import { BonusTransaction } from '../models/BonusTransaction';
import { User } from '../models/User';

function httpError(message: string, status: number): Error & { status: number } {
  const error = new Error(message) as Error & { status: number };
  error.status = status;
  return error;
}

export async function getUserById(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  try {
    const user = await User.findByPk(req.params.id);

    if (!user) {
      throw httpError('User not found', 404);
    }

    res.json(user);
  } catch (error) {
    next(error);
  }
}

export async function getUserBonusTransactions(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  try {
    const transactions = await BonusTransaction.findAll({
      where: { user_id: req.params.id },
      order: [['created_at', 'DESC']],
    });

    res.json(transactions);
  } catch (error) {
    next(error);
  }
}
