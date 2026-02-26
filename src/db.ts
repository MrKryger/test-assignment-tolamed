import { Sequelize } from 'sequelize';

import { initBonusTransactionModel, BonusTransaction } from './models/BonusTransaction';
import { initUserModel, User } from './models/User';

const DB_HOST = process.env.DB_HOST || 'postgres';
const DB_PORT = Number(process.env.DB_PORT || 5432);
const DB_NAME = process.env.DB_NAME || 'appdb';
const DB_USER = process.env.DB_USER || 'app';
const DB_PASSWORD = process.env.DB_PASSWORD || 'app';

export const sequelize = new Sequelize(DB_NAME, DB_USER, DB_PASSWORD, {
  host: DB_HOST,
  port: DB_PORT,
  dialect: 'postgres',
  logging: false,
});

initUserModel(sequelize);
initBonusTransactionModel(sequelize);

User.hasMany(BonusTransaction, {
  foreignKey: 'user_id',
  as: 'bonusTransactions',
});

BonusTransaction.belongsTo(User, {
  foreignKey: 'user_id',
  as: 'user',
});
