'use strict';

module.exports = {
  async up(queryInterface, Sequelize) {
    const tables = await queryInterface.showAllTables();
    const transactionsTableExists = tables.some((table) => {
      if (typeof table === 'string') {
        return table === 'bonus_transactions';
      }

      return table.tableName === 'bonus_transactions';
    });

    if (transactionsTableExists) {
      return;
    }

    await queryInterface.createTable('bonus_transactions', {
      id: {
        type: Sequelize.UUID,
        allowNull: false,
        primaryKey: true,
        defaultValue: Sequelize.literal('gen_random_uuid()'),
      },
      user_id: {
        type: Sequelize.UUID,
        allowNull: false,
        references: {
          model: 'users',
          key: 'id',
        },
        onUpdate: 'CASCADE',
        onDelete: 'CASCADE',
      },
      type: {
        type: Sequelize.STRING,
        allowNull: false,
      },
      amount: {
        type: Sequelize.INTEGER,
        allowNull: false,
      },
      expires_at: {
        type: Sequelize.DATE,
        allowNull: true,
      },
      request_id: {
        type: Sequelize.STRING,
        allowNull: true,
      },
      created_at: {
        type: Sequelize.DATE,
        allowNull: false,
        defaultValue: Sequelize.literal('NOW()'),
      },
      updated_at: {
        type: Sequelize.DATE,
        allowNull: false,
        defaultValue: Sequelize.literal('NOW()'),
      },
    });

    await queryInterface.sequelize.query(`
      ALTER TABLE bonus_transactions
      ADD CONSTRAINT bonus_transactions_type_chk CHECK (type IN ('accrual', 'spend')),
      ADD CONSTRAINT bonus_transactions_amount_positive_chk CHECK (amount > 0);
    `);

    await queryInterface.addIndex('bonus_transactions', ['user_id', 'created_at'], {
      name: 'bonus_transactions_user_id_created_at_idx',
    });

    await queryInterface.addIndex('bonus_transactions', ['request_id'], {
      name: 'bonus_transactions_request_id_uq',
      unique: true,
    });
  },

  async down(queryInterface) {
    await queryInterface.sequelize.query('DROP TABLE IF EXISTS bonus_transactions CASCADE;');
  },
};
