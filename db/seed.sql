CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bonus_transactions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE,
  type VARCHAR(20) NOT NULL CHECK (type IN ('accrual', 'spend')),
  amount INTEGER NOT NULL CHECK (amount > 0),
  expires_at TIMESTAMP NULL,
  request_id VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS bonus_transactions_user_id_created_at_idx
  ON bonus_transactions (user_id, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS bonus_transactions_request_id_uq
  ON bonus_transactions (request_id);

INSERT INTO users (id, name, created_at, updated_at)
VALUES
  ('11111111-1111-1111-1111-111111111111', 'Alice', NOW(), NOW()),
  ('22222222-2222-2222-2222-222222222222', 'Bob', NOW(), NOW()),
  ('33333333-3333-3333-3333-333333333333', 'Charlie', NOW(), NOW())
ON CONFLICT (id) DO NOTHING;

INSERT INTO bonus_transactions (id, user_id, type, amount, expires_at, request_id, created_at, updated_at)
VALUES
  (
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    '11111111-1111-1111-1111-111111111111',
    'accrual',
    300,
    NOW() + INTERVAL '30 days',
    NULL,
    NOW(),
    NOW()
  ),
  (
    'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
    '11111111-1111-1111-1111-111111111111',
    'spend',
    50,
    NULL,
    'request-spend-001',
    NOW(),
    NOW()
  ),
  (
    'cccccccc-cccc-cccc-cccc-cccccccccccc',
    '11111111-1111-1111-1111-111111111111',
    'accrual',
    100,
    NOW() - INTERVAL '30 days',
    NULL,
    NOW(),
    NOW()
  ),
  (
    'dddddddd-dddd-dddd-dddd-dddddddddddd',
    '22222222-2222-2222-2222-222222222222',
    'accrual',
    1000,
    NOW() + INTERVAL '90 days',
    NULL,
    NOW(),
    NOW()
  )
ON CONFLICT (id) DO NOTHING;
