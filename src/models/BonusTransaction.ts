import {
  CreationOptional,
  DataTypes,
  InferAttributes,
  InferCreationAttributes,
  Model,
  Sequelize,
} from 'sequelize';

export class BonusTransaction extends Model<
  InferAttributes<BonusTransaction>,
  InferCreationAttributes<BonusTransaction>
> {
  declare id: CreationOptional<string>;
  declare user_id: string;
  declare type: 'accrual' | 'spend';
  declare amount: number;
  declare expires_at: Date | null;
  declare request_id: string | null;
  declare created_at: CreationOptional<Date>;
  declare updated_at: CreationOptional<Date>;
}

export function initBonusTransactionModel(sequelize: Sequelize): void {
  BonusTransaction.init(
    {
      id: {
        type: DataTypes.UUID,
        allowNull: false,
        primaryKey: true,
        defaultValue: DataTypes.UUIDV4,
      },
      user_id: {
        type: DataTypes.UUID,
        allowNull: false,
      },
      type: {
        type: DataTypes.STRING,
        allowNull: false,
        validate: {
          isIn: [['accrual', 'spend']],
        },
      },
      amount: {
        type: DataTypes.INTEGER,
        allowNull: false,
        validate: {
          min: 1,
        },
      },
      expires_at: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      request_id: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      created_at: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
      updated_at: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
    },
    {
      sequelize,
      tableName: 'bonus_transactions',
      modelName: 'BonusTransaction',
      underscored: true,
      timestamps: true,
      createdAt: 'created_at',
      updatedAt: 'updated_at',
    },
  );
}
