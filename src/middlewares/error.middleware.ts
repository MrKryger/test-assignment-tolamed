import { NextFunction, Request, Response } from 'express';

type ErrorWithStatus = Error & {
  status?: number;
};

export function errorMiddleware(
  err: ErrorWithStatus,
  _req: Request,
  res: Response,
  _next: NextFunction,
): void {
  const status = err.status ?? 500;

  res.status(status).json({
    message: err.message || 'Internal Server Error',
  });
}
