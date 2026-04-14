CREATE TYPE Institution_Type AS ENUM ('Bank', 'Credit Union', 'Insurance', 'Pension Fund', 'Broker', 'Crypto Exchange', 'Internal', 'Other');
CREATE TYPE Account_Type AS ENUM ('Cash', 'Checking', 'Savings', 'Credit Card', 'Brokerage', 'Pension', 'Other Investment', 'Margin', 'Loan', 'Real Estate', 'Vehicle', 'Asset', 'Liability', 'Other');
CREATE TYPE Security_Type AS ENUM ('Stock', 'ETF', 'Bond', 'Mutual Fund', 'Crypto', 'Option', 'Commodity', 'PF_Unit');
CREATE TYPE Transaction_Category_Type AS ENUM ('Income', 'Expense', 'Transfer', 'Investment_Buy', 'Investment_Sell', 'Dividend', 'Interest', 'Tax', 'Fee');
CREATE TYPE Investment_Action AS ENUM ('Buy', 'Sell', 'Dividend', 'Reinvest', 'Split', 'ShrIn', 'ShrOut', 'IntInc', 'CashIn', 'CashOut', 'Vest', 'Expire', 'Grant', 'Exercise', 'MiscExp', 'RtrnCap');
CREATE SEQUENCE IF NOT EXISTS transfer_id_seq START 1 INCREMENT 1;

CREATE TABLE Currencies (
    Currencies_Id SERIAL PRIMARY KEY,
    Currencies_ShortName CHAR(3) UNIQUE NOT NULL, -- EUR, USD, GBP, BTC
    Currencies_Name VARCHAR(100) NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_currency_id ON Currencies(Currencies_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_currency_name ON Currencies(Currencies_ShortName);

CREATE TABLE FinancialInstitutions (
    FinancialInstitutions_Id SERIAL PRIMARY KEY,
    FinancialInstitutions_Name VARCHAR(100) NOT NULL,
    FinancialInstitutions_Type Institution_Type NOT NULL,
    BIC_Code VARCHAR(11),
    Contact VARCHAR(100),
    Phone VARCHAR(20),
    Email VARCHAR(100),
    Website VARCHAR(255),
    Notes TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_institution_id ON FinancialInstitutions(FinancialInstitutions_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_institution_name ON FinancialInstitutions(FinancialInstitutions_Name);

CREATE TABLE Categories (
    Categories_Id SERIAL PRIMARY KEY,
    Categories_Name VARCHAR(100) NOT NULL,
    Parent_Category_Id INTEGER REFERENCES Categories(Categories_Id) ON DELETE CASCADE,
    Category_Type Transaction_Category_Type NOT NULL,
    UNIQUE(Categories_Name, Parent_Category_Id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_category_id ON Categories(Categories_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_category_name ON Categories(Categories_Name, Parent_Category_Id);

CREATE TABLE Securities (
    Securities_Id SERIAL PRIMARY KEY,
    Ticker VARCHAR(255) UNIQUE NOT NULL,          -- π.χ. 'AAPL', 'BTC-USD', 'EURUSD=X'
    Security_Name VARCHAR(255) UNIQUE NOT NULL,
    Security_Type Security_Type NOT NULL,
    Currencies_Id INTEGER REFERENCES Currencies(Currencies_Id),
    Sector VARCHAR(50),
    Is_Active BOOLEAN DEFAULT TRUE,
    Yahoo_Ticker VARCHAR(30)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_security_id ON Securities(Securities_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_security_name ON Securities(Security_Name);

CREATE TABLE Accounts (
    Accounts_Id SERIAL PRIMARY KEY,
    Accounts_Name VARCHAR(100) NOT NULL,
    Accounts_Type Account_Type NOT NULL,
    Institution_Id INTEGER REFERENCES FinancialInstitutions(FinancialInstitutions_Id),
    IBAN VARCHAR(34),
    Currencies_Id INTEGER REFERENCES Currencies(Currencies_Id),
    Account_Balance NUMERIC(28, 18) DEFAULT 0, -- Υψηλή ακρίβεια για Crypto/Satoshi
    Is_Active BOOLEAN DEFAULT TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_account_id ON Accounts(Accounts_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_account_name ON Accounts(Accounts_Name);

CREATE TABLE Holdings (
    Holdings_Id SERIAL PRIMARY KEY,
    Accounts_Id INTEGER REFERENCES Accounts(Accounts_Id) ON DELETE CASCADE,
    Securities_Id INTEGER REFERENCES Securities(Securities_Id),
    Quantity NUMERIC(28, 18) NOT NULL DEFAULT 0,
    Simple_Avg_Price NUMERIC(20, 8),               
    Fifo_Avg_Price NUMERIC(20, 8),               
    Last_Update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(Accounts_Id, Securities_Id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_holding_id ON Holdings(Holdings_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_holding_accsec ON Holdings(Accounts_Id, Securities_Id);

-- 1. Δημιουργία Πίνακα Payees
CREATE TABLE Payees (
    Payees_Id SERIAL PRIMARY KEY,
    Payees_Name VARCHAR(255) UNIQUE NOT NULL,
    Default_Categories_Id INTEGER REFERENCES Categories(Categories_Id), -- Προαιρετικό: Αυτόματη κατηγορία
    Notes TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_payee_id ON Payees(Payees_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_payee_name ON Payees(Payees_Name);

-- Κύριος πίνακας (Bank & Credit Cards)
CREATE TABLE Bank_Transactions (
    Bank_Transactions_Id SERIAL PRIMARY KEY,
    Accounts_Id INTEGER REFERENCES Accounts(Accounts_Id) ON DELETE CASCADE,
    Date DATE NOT NULL,              -- Εδώ θα μπαίνουν και μελλοντικές ημερομηνίες για δόσεις
    Payees_Id INTEGER REFERENCES Payees(Payees_Id),
    Description TEXT,                -- π.χ. "Αγορά Τηλεόρασης - Δόση 1/12"
    Total_Amount NUMERIC(28, 18),    -- Συνολικό ποσό κίνησης
    Cleared BOOLEAN DEFAULT FALSE,    -- FALSE για τις μελλοντικές δόσεις
	Transfer_Id INTEGER NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transaction_id ON Bank_Transactions(Bank_Transactions_Id);
CREATE INDEX IF NOT EXISTS idx_transfer_id ON Bank_Transactions(Transfer_Id) WHERE Transfer_Id IS NOT NULL;

-- Πίνακας Splits (Εδώ γίνεται η ανάλυση κατηγοριών)
CREATE TABLE Bank_Transaction_Splits (
    Split_Id SERIAL PRIMARY KEY,
    Bank_Transactions_Id INTEGER REFERENCES Bank_Transactions(Bank_Transactions_Id) ON DELETE CASCADE,
    Categories_Id INTEGER REFERENCES Categories(Categories_Id),
    Amount NUMERIC(28, 18),
    Memo TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_split_id ON Bank_Transaction_Splits(Split_Id);
CREATE INDEX IF NOT EXISTS idx_split_tx_id ON Bank_Transaction_Splits(Bank_Transactions_Id);


CREATE OR REPLACE FUNCTION update_account_balance()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        UPDATE Accounts SET Account_Balance = Account_Balance + NEW.Total_Amount 
        WHERE Accounts_Id = NEW.Accounts_Id;
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE Accounts SET Account_Balance = Account_Balance - OLD.Total_Amount 
        WHERE Accounts_Id = OLD.Accounts_Id;
    ELSIF (TG_OP = 'UPDATE') THEN
        -- 1. Αφαιρούμε το παλιό ποσό από τον ΠΑΛΙΟ λογαριασμό
        UPDATE Accounts SET Account_Balance = Account_Balance - OLD.Total_Amount 
        WHERE Accounts_Id = OLD.Accounts_Id;
        -- 2. Προσθέτουμε το νέο ποσό στον ΝΕΟ λογαριασμό
        UPDATE Accounts SET Account_Balance = Account_Balance + NEW.Total_Amount 
        WHERE Accounts_Id = NEW.Accounts_Id;		
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_balance
AFTER INSERT OR UPDATE OR DELETE ON Bank_Transactions
FOR EACH ROW EXECUTE FUNCTION update_account_balance();


CREATE TABLE Investment_Transactions (
    Inv_Transactions_Id SERIAL PRIMARY KEY,
    Accounts_Id INTEGER REFERENCES Accounts(Accounts_Id) ON DELETE CASCADE,
    Securities_Id INTEGER REFERENCES Securities(Securities_Id),
    Date DATE NOT NULL,
    Action Investment_Action NOT NULL,
    Quantity NUMERIC(28, 18),         -- Αριθμός μετοχών
    Price_Per_Share NUMERIC(20, 8),
    Commission NUMERIC(20, 8) DEFAULT 0,
    Total_Amount NUMERIC(28, 18),      -- Συνολικό ποσό μετρητών που κινήθηκε
    Description TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_inv_transaction_id ON Investment_Transactions(Inv_Transactions_Id);

-- FUNCTION: public.update_holdings_from_investment()

-- DROP FUNCTION IF EXISTS public.update_holdings_from_investment();

CREATE OR REPLACE FUNCTION public.update_holdings_from_investment()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- Εκτέλεση του FIFO/Simple Average υπολογισμού μόνο για το συγκεκριμένο Account & Security
    WITH TransactionFlow AS (
        SELECT 
            Accounts_Id, 
            Securities_Id,
            Date,
            Inv_Transactions_Id,
            Action,
            Quantity,
            Price_Per_Share,
            -- Ιστορικός Μέσος Όρος (Simple Average)
            AVG(Price_Per_Share) FILTER (WHERE Action IN ('Buy', 'Reinvest', 'ShrIn')) 
                OVER (PARTITION BY Accounts_Id, Securities_Id) as simple_avg_cost,
            -- Σωρευτικά FIFO
            SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity ELSE 0 END) 
                OVER (PARTITION BY Accounts_Id, Securities_Id ORDER BY Date, Inv_Transactions_Id) as running_buys,
            SUM(CASE WHEN Action IN ('Sell', 'ShrOut') THEN Quantity ELSE 0 END) 
                OVER (PARTITION BY Accounts_Id, Securities_Id ORDER BY Date, Inv_Transactions_Id) as running_sells,
            SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity ELSE 0 END) 
                OVER (PARTITION BY Accounts_Id, Securities_Id) as total_buys,
            SUM(CASE WHEN Action IN ('Sell', 'ShrOut') THEN Quantity ELSE 0 END) 
                OVER (PARTITION BY Accounts_Id, Securities_Id) as total_sells
        FROM Investment_Transactions
        WHERE Accounts_Id = NEW.Accounts_Id AND Securities_Id = NEW.Securities_Id -- Performance Filter
          AND Action IN ('Buy', 'Reinvest', 'ShrIn', 'Sell', 'ShrOut')
    ),
    FIFO_Positions AS (
        SELECT 
            Accounts_Id, Securities_Id, simple_avg_cost, Price_Per_Share,
            CASE 
                WHEN total_buys >= total_sells THEN 
                    CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN
                        CASE WHEN running_buys <= total_sells THEN 0
                             WHEN running_buys - Quantity < total_sells THEN running_buys - total_sells
                             ELSE Quantity END
                    ELSE 0 END
                ELSE -- SHORT CASE
                    CASE WHEN Action IN ('Sell', 'ShrOut') THEN
                        CASE WHEN running_sells <= total_buys THEN 0
                             WHEN running_sells - Quantity < total_buys THEN -(running_sells - total_buys)
                             ELSE -Quantity END
                    ELSE 0 END
            END as remaining_qty
        FROM TransactionFlow
    )
    INSERT INTO Holdings (Accounts_Id, Securities_Id, Quantity, Simple_Avg_Price, Fifo_Avg_Price, Last_Update)
    SELECT 
        Accounts_Id, 
        Securities_Id, 
        SUM(remaining_qty),
        CASE WHEN ABS(SUM(remaining_qty)) > 0 THEN SUM(ABS(remaining_qty) * Price_Per_Share) / SUM(ABS(remaining_qty)) ELSE 0 END,
        MAX(simple_avg_cost),
        CURRENT_TIMESTAMP
    FROM FIFO_Positions
    GROUP BY Accounts_Id, Securities_Id
    ON CONFLICT (Accounts_Id, Securities_Id) 
    DO UPDATE SET 
        Quantity = EXCLUDED.Quantity,
        Simple_Avg_Price = EXCLUDED.Simple_Avg_Price,
        Fifo_Avg_Price = EXCLUDED.Fifo_Avg_Price,
        Last_Update = EXCLUDED.Last_Update;

    -- Καθαρισμός αν η θέση έκλεισε (Quantity = 0)
    DELETE FROM Holdings WHERE Accounts_Id = NEW.Accounts_Id AND Securities_Id = NEW.Securities_Id AND Quantity = 0;

    RETURN NEW;
END;
$BODY$;

ALTER FUNCTION public.update_holdings_from_investment()
    OWNER TO admin;



-- Ιστορικό τιμών για Μετοχές, ETFs, Crypto, PF Units
CREATE TABLE Historical_Prices (
    Securities_Id INTEGER REFERENCES Securities(Securities_Id) ON DELETE CASCADE,
    Price_Date DATE NOT NULL,
    Price_Close NUMERIC(20, 8) NOT NULL,
    Volume BIGINT,
    PRIMARY KEY (Securities_Id, Price_Date)
);
ALTER TABLE Historical_Prices ADD UNIQUE (Securities_Id, Price_Date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_price_id ON Historical_Prices(Securities_Id, Price_Date);

-- Ιστορικό ισοτιμιών (FX Rates)
CREATE TABLE Historical_FX (
    Base_Currency_Id INTEGER REFERENCES Currencies(Currencies_Id),   -- π.χ. EUR
    Target_Currency_Id INTEGER REFERENCES Currencies(Currencies_Id), -- π.χ. GBP
    FX_Date DATE NOT NULL,
    FX_Rate NUMERIC(18, 10) NOT NULL,
    PRIMARY KEY (Base_Currency_Id, Target_Currency_Id, FX_Date)
);
ALTER TABLE Historical_FX ADD UNIQUE (Base_Currency_Id, Target_Currency_Id, FX_Date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fxrate_id ON Historical_FX(Base_Currency_Id, Target_Currency_Id, FX_Date);


CREATE EXTENSION IF NOT EXISTS vector;
ALTER TABLE Accounts 
ADD COLUMN embedding vector(768);
ALTER TABLE Bank_Transaction_Splits 
ADD COLUMN embedding vector(768);
ALTER TABLE Bank_Transactions 
ADD COLUMN embedding vector(768);
ALTER TABLE Categories 
ADD COLUMN embedding vector(768);
ALTER TABLE Currencies 
ADD COLUMN embedding vector(768);
ALTER TABLE FinancialInstitutions 
ADD COLUMN embedding vector(768);
ALTER TABLE Historical_FX 
ADD COLUMN embedding vector(768);
ALTER TABLE Historical_Prices 
ADD COLUMN embedding vector(768);
ALTER TABLE Holdings 
ADD COLUMN embedding vector(768);
ALTER TABLE Investment_Transactions 
ADD COLUMN embedding vector(768);
ALTER TABLE Payees 
ADD COLUMN embedding vector(768);
ALTER TABLE Securities 
ADD COLUMN embedding vector(768);

ALTER DATABASE "Finance" REFRESH COLLATION VERSION;
