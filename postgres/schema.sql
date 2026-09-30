-- ============================================================
-- Banking OLTP schema (source system for CDC)
--
-- Idempotent: safe to run on a fresh database AND re-run on an
-- existing one. New constraints are added NOT VALID so they are
-- enforced for new rows without failing on historical data.
-- ============================================================

CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    account_type VARCHAR(20) NOT NULL,
    balance NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    transaction_type VARCHAR(50) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    related_account_id INT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- ------------------------------------------------------------
-- updated_at: lets downstream order CDC versions and audit changes
-- ------------------------------------------------------------
ALTER TABLE customers    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now();
ALTER TABLE accounts     ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now();
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now();

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER customers_set_updated_at
    BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE OR REPLACE TRIGGER accounts_set_updated_at
    BEFORE UPDATE ON accounts FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE OR REPLACE TRIGGER transactions_set_updated_at
    BEFORE UPDATE ON transactions FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ------------------------------------------------------------
-- Domain constraints (NOT VALID = enforced for new rows only)
-- ------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'accounts_account_type_check') THEN
        ALTER TABLE accounts ADD CONSTRAINT accounts_account_type_check
            CHECK (account_type IN ('SAVINGS', 'CHECKING')) NOT VALID;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transactions_type_check') THEN
        ALTER TABLE transactions ADD CONSTRAINT transactions_type_check
            CHECK (transaction_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER')) NOT VALID;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transactions_status_check') THEN
        ALTER TABLE transactions ADD CONSTRAINT transactions_status_check
            CHECK (status IN ('COMPLETED', 'FAILED')) NOT VALID;
    END IF;

    -- A TRANSFER must name a counterparty; other types must not.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transactions_counterparty_check') THEN
        ALTER TABLE transactions ADD CONSTRAINT transactions_counterparty_check
            CHECK (
                (transaction_type = 'TRANSFER' AND related_account_id IS NOT NULL AND related_account_id <> account_id)
                OR (transaction_type <> 'TRANSFER' AND related_account_id IS NULL)
            ) NOT VALID;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transactions_related_account_id_fkey') THEN
        ALTER TABLE transactions ADD CONSTRAINT transactions_related_account_id_fkey
            FOREIGN KEY (related_account_id) REFERENCES accounts(id) NOT VALID;
    END IF;
END;
$$;

-- ------------------------------------------------------------
-- Indexes (Postgres does not index foreign keys automatically)
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_accounts_customer_id          ON accounts (customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_account_id       ON transactions (account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_related_account  ON transactions (related_account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at       ON transactions (created_at);

-- ------------------------------------------------------------
-- CDC: emit full before-images for UPDATE/DELETE events
-- ------------------------------------------------------------
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
