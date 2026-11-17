-- Source schema: 12 bank tables + 1 instrumentation table (canary)

CREATE TABLE branches (
  branch_id    INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  code         VARCHAR(10) NOT NULL UNIQUE,
  name         VARCHAR(100) NOT NULL,
  city         VARCHAR(60) NOT NULL,
  ifsc         CHAR(11) NOT NULL UNIQUE,
  opened_on    DATE NOT NULL
);

CREATE TABLE employees (
  employee_id  INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  branch_id    INT NOT NULL REFERENCES branches(branch_id),
  name         VARCHAR(100) NOT NULL,
  role         TEXT NOT NULL CHECK (role IN ('manager','officer','teller')),
  email        VARCHAR(120) NOT NULL UNIQUE,
  hired_on     DATE NOT NULL
);

CREATE TABLE customers (
  customer_id  INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name         VARCHAR(100) NOT NULL,
  email        VARCHAR(120) NOT NULL UNIQUE,
  phone        VARCHAR(15) NOT NULL,
  dob          DATE NOT NULL,
  kyc_status   TEXT NOT NULL DEFAULT 'pending' CHECK (kyc_status IN ('pending','verified','rejected')),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE addresses (
  address_id   INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id  INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
  type         TEXT NOT NULL CHECK (type IN ('home','work')),
  line1        VARCHAR(200) NOT NULL,
  city         VARCHAR(60) NOT NULL,
  state        VARCHAR(60) NOT NULL,
  pincode      CHAR(6) NOT NULL
);

CREATE TABLE account_types (
  type_id        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  code           VARCHAR(10) NOT NULL UNIQUE,
  name           VARCHAR(60) NOT NULL,
  interest_rate  NUMERIC(5,2) NOT NULL,
  min_balance    NUMERIC(15,2) NOT NULL
);

CREATE TABLE accounts (
  account_id   INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_no   CHAR(12) NOT NULL UNIQUE,
  type_id      INT NOT NULL REFERENCES account_types(type_id),
  branch_id    INT NOT NULL REFERENCES branches(branch_id),
  balance      NUMERIC(15,2) NOT NULL DEFAULT 0,
  status       TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','frozen','closed')),
  opened_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE account_holders (
  account_id   INT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  customer_id  INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
  role         TEXT NOT NULL CHECK (role IN ('primary','joint')),
  PRIMARY KEY (account_id, customer_id)
);

CREATE TABLE cards (
  card_id      INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_id   INT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  last4        CHAR(4) NOT NULL,
  network      TEXT NOT NULL CHECK (network IN ('visa','mastercard','rupay')),
  expires_on   DATE NOT NULL,
  status       TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','blocked'))
);

CREATE TABLE transactions (
  txn_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_id    INT NOT NULL REFERENCES accounts(account_id),
  type          TEXT NOT NULL CHECK (type IN ('credit','debit')),
  amount        NUMERIC(15,2) NOT NULL,
  balance_after NUMERIC(15,2) NOT NULL,
  description   VARCHAR(200),
  counterparty  CHAR(12),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE beneficiaries (
  beneficiary_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id    INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
  name           VARCHAR(100) NOT NULL,
  account_no     CHAR(12) NOT NULL,
  ifsc           CHAR(11) NOT NULL,
  added_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE loans (
  loan_id        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id    INT NOT NULL REFERENCES customers(customer_id),
  branch_id      INT NOT NULL REFERENCES branches(branch_id),
  principal      NUMERIC(15,2) NOT NULL,
  rate           NUMERIC(5,2) NOT NULL,
  tenure_months  SMALLINT NOT NULL,
  status         TEXT NOT NULL CHECK (status IN ('active','closed','defaulted')),
  disbursed_on   DATE NOT NULL
);

CREATE TABLE loan_payments (
  payment_id   INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  loan_id      INT NOT NULL REFERENCES loans(loan_id) ON DELETE CASCADE,
  due_date     DATE NOT NULL,
  paid_on      DATE,
  amount       NUMERIC(15,2) NOT NULL,
  status       TEXT NOT NULL CHECK (status IN ('due','paid','late'))
);

-- Instrumentation only: measures end-to-end replication lag
CREATE TABLE canary (
  id          SMALLINT PRIMARY KEY,
  written_ms  BIGINT NOT NULL
);
INSERT INTO canary VALUES (1, 0);

-- PostgreSQL does not index foreign-key columns automatically
CREATE INDEX ON employees (branch_id);
CREATE INDEX ON addresses (customer_id);
CREATE INDEX ON accounts (type_id);
CREATE INDEX ON accounts (branch_id);
CREATE INDEX ON account_holders (customer_id);
CREATE INDEX ON cards (account_id);
CREATE INDEX ON transactions (account_id, created_at);
CREATE INDEX ON beneficiaries (customer_id);
CREATE INDEX ON loans (customer_id);
CREATE INDEX ON loan_payments (loan_id);
