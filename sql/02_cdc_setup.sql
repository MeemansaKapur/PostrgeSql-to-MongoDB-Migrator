-- Least-privilege CDC role: replication + read-only
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'dbz_password_change_me';
GRANT CONNECT ON DATABASE bank TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;

-- We create the publication ourselves (publication.autocreate.mode=disabled)
CREATE PUBLICATION dbz_publication FOR TABLE
  branches, employees, customers, addresses, account_types, accounts,
  account_holders, cards, transactions, beneficiaries, loans, loan_payments, canary;

-- Full old-row images where handlers need the old foreign key (deletes, parent moves)
ALTER TABLE addresses       REPLICA IDENTITY FULL;
ALTER TABLE beneficiaries   REPLICA IDENTITY FULL;
ALTER TABLE cards           REPLICA IDENTITY FULL;
ALTER TABLE loan_payments   REPLICA IDENTITY FULL;
ALTER TABLE employees       REPLICA IDENTITY FULL;
ALTER TABLE account_holders REPLICA IDENTITY FULL;
