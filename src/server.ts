import 'dotenv/config';

import app from './app';
import { sequelize } from './db';
import { startExpireAccrualsWorker } from './queue';

const port = Number(process.env.PORT || 3000);

async function startServer(): Promise<void> {
  try {
    await sequelize.authenticate();
    console.log('PostgreSQL connected');

    startExpireAccrualsWorker();

    app.listen(port, () => {
      console.log(`API started on port ${port}`);
    });
  } catch (error) {
    console.error('Failed to start server', error);
    process.exit(1);
  }
}

void startServer();
