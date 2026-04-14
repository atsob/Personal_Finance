-- Add Transfer_Id column to Bank_Transactions table
-- This column links transfer source and destination transactions

-- Create a sequence for Transfer_Id
CREATE SEQUENCE transfer_id_seq START 1 INCREMENT 1;

-- Add the Transfer_Id column (nullable, only used for transfers)
ALTER TABLE Bank_Transactions 
ADD COLUMN Transfer_Id BIGINT;

-- Add index for faster lookups
CREATE INDEX idx_transfer_id ON Bank_Transactions(Transfer_Id) WHERE Transfer_Id IS NOT NULL;
