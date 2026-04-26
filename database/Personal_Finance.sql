
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TYPE Institutions_Type AS ENUM ('Bank', 'Credit Union', 'Insurance', 'Pension Fund', 'Broker', 'Crypto Exchange', 'Internal', 'Other');
CREATE TYPE Accounts_Type AS ENUM ('Cash', 'Checking', 'Savings', 'Credit Card', 'Brokerage', 'Pension', 'Other Investment', 'Margin', 'Loan', 'Real Estate', 'Vehicle', 'Asset', 'Liability', 'Other');
CREATE TYPE Securities_Type AS ENUM ('Stock', 'ETF', 'Bond', 'Mutual Fund', 'Crypto', 'Option', 'Commodity', 'PF_Unit');
CREATE TYPE Categories_Type AS ENUM ('Income', 'Expense', 'Transfer', 'Investment_Buy', 'Investment_Sell', 'Dividend', 'Interest', 'Tax', 'Fee');
CREATE TYPE Investments_Action AS ENUM ('Buy', 'Sell', 'Dividend', 'Reinvest', 'Split', 'ShrIn', 'ShrOut', 'IntInc', 'CashIn', 'CashOut', 'Vest', 'Expire', 'Grant', 'Exercise', 'MiscExp', 'RtrnCap');
CREATE SEQUENCE IF NOT EXISTS transfers_id_seq START 1 INCREMENT 1;

CREATE TABLE Currencies (
    Currencies_Id SERIAL PRIMARY KEY,
    Currencies_ShortName CHAR(3) UNIQUE NOT NULL, -- EUR, USD, GBP, BTC
    Currencies_Name VARCHAR(100) NOT NULL,
    embedding vector(768)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_currencies_id ON Currencies(Currencies_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_currencies_shortname ON Currencies(Currencies_ShortName);

CREATE TABLE IF NOT EXISTS Credit_Ratings_LT (
    Credit_Ratings_LT_Id integer PRIMARY KEY,
    Quality VARCHAR(25) NOT NULL,
    Description VARCHAR(25) NOT NULL,
    Moodys VARCHAR(4) UNIQUE NOT NULL,
    S_P VARCHAR(4) UNIQUE NOT NULL,
    Fitch VARCHAR(4) UNIQUE NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_credit_ratings_lt_id ON Credit_Ratings_LT(Credit_Ratings_LT_Id);
--CREATE UNIQUE INDEX IF NOT EXISTS idx_moodys_id ON Credit_Ratings_LT(Moodys);
--CREATE UNIQUE INDEX IF NOT EXISTS idx_s_p_id ON Credit_Ratings_LT(S_P);
--CREATE UNIQUE INDEX IF NOT EXISTS idx_fitch_id ON Credit_Ratings_LT(Fitch);

CREATE TABLE Institutions (
    Institutions_Id SERIAL PRIMARY KEY,
    Institutions_Name VARCHAR(100) NOT NULL,
    Institutions_Type Institutions_Type NOT NULL,
    BIC_Code VARCHAR(11),
    Moodys VARCHAR(4) REFERENCES Credit_Ratings_LT(Moodys),
    S_P VARCHAR(4) REFERENCES Credit_Ratings_LT(S_P),
    Fitch VARCHAR(4) REFERENCES Credit_Ratings_LT(Fitch),
    Contact VARCHAR(100),
    Phone VARCHAR(20),
    Email VARCHAR(100),
    Website VARCHAR(255),
    Notes TEXT,
    embedding vector(768)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_institutions_id ON Institutions(Institutions_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_institutions_name ON Institutions(Institutions_Name);

CREATE TABLE Categories (
    Categories_Id SERIAL PRIMARY KEY,
    Categories_Name VARCHAR(100) NOT NULL,
    Categories_Id_Parent INTEGER REFERENCES Categories(Categories_Id),
    Categories_Type Categories_Type NOT NULL,
    embedding vector(768),
    UNIQUE(Categories_Name, Categories_Id_Parent)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_id ON Categories(Categories_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_name ON Categories(Categories_Name, Categories_Id_Parent);

CREATE TABLE Securities (
    Securities_Id SERIAL PRIMARY KEY,
    Ticker VARCHAR(255) UNIQUE NOT NULL,          -- π.χ. 'AAPL', 'BTC-USD', 'EURUSD=X'
    Securities_Name VARCHAR(255) UNIQUE NOT NULL,
    Securities_Type Securities_Type NOT NULL,
    Currencies_Id INTEGER NOT NULL REFERENCES Currencies(Currencies_Id),
    Sector VARCHAR(50),
    Industry VARCHAR(50),
    Analyst_Rating VARCHAR(20),
    Analyst_Target_Price NUMERIC(20, 8),
    Is_Active BOOLEAN DEFAULT TRUE,
    Yahoo_Ticker VARCHAR(30),
    embedding vector(768)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_securities_id ON Securities(Securities_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_securities_name ON Securities(Securities_Name);

CREATE TABLE Accounts (
    Accounts_Id SERIAL PRIMARY KEY,
    Accounts_Name VARCHAR(100) UNIQUE NOT NULL,
    Accounts_Type Accounts_Type NOT NULL,
    Institutions_Id INTEGER REFERENCES Institutions(Institutions_Id),
    IBAN VARCHAR(34),
    Currencies_Id INTEGER NOT NULL REFERENCES Currencies(Currencies_Id),
	Credit_Limit NUMERIC(18, 2) DEFAULT 0,
    Accounts_Balance NUMERIC(28, 18) DEFAULT 0, -- Υψηλή ακρίβεια για Crypto/Satoshi
    Is_Active BOOLEAN DEFAULT TRUE,
    embedding vector(768)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_id ON Accounts(Accounts_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_name ON Accounts(Accounts_Name);

CREATE TABLE Holdings (
    Holdings_Id SERIAL PRIMARY KEY,
    Accounts_Id INTEGER NOT NULL REFERENCES Accounts(Accounts_Id),
    Securities_Id INTEGER NOT NULL REFERENCES Securities(Securities_Id),
    Quantity NUMERIC(28, 18) NOT NULL DEFAULT 0,
    Simple_Avg_Price NUMERIC(20, 8),               
    Fifo_Avg_Price NUMERIC(20, 8),               
    Last_Update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    embedding vector(768),
    UNIQUE(Accounts_Id, Securities_Id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_holdings_id ON Holdings(Holdings_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_holdings_accsec ON Holdings(Accounts_Id, Securities_Id);

-- 1. Δημιουργία Πίνακα Payees
CREATE TABLE Payees (
    Payees_Id SERIAL PRIMARY KEY,
    Payees_Name VARCHAR(255) UNIQUE NOT NULL,
    Categories_Id_Default INTEGER REFERENCES Categories(Categories_Id), -- Προαιρετικό: Αυτόματη κατηγορία
    Notes TEXT,
    embedding vector(768)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_payees_id ON Payees(Payees_Id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_payees_name ON Payees(Payees_Name);

-- Κύριος πίνακας (Bank & Credit Cards)
CREATE TABLE Transactions (
    Transactions_Id SERIAL PRIMARY KEY,
    Accounts_Id INTEGER NOT NULL REFERENCES Accounts(Accounts_Id),
    Date DATE NOT NULL,              -- Εδώ θα μπαίνουν και μελλοντικές ημερομηνίες για δόσεις
    Payees_Id INTEGER REFERENCES Payees(Payees_Id),
    Description TEXT,                -- π.χ. "Αγορά Τηλεόρασης - Δόση 1/12"
    Total_Amount NUMERIC(28, 18),    -- Συνολικό ποσό κίνησης
    Cleared BOOLEAN DEFAULT FALSE,    -- FALSE για τις μελλοντικές δόσεις
    Accounts_Id_Target INTEGER REFERENCES Accounts(Accounts_Id),
    Total_Amount_Target NUMERIC(28, 18),    -- Συνολικό ποσό κίνησης
	Transfers_Id INTEGER NULL,
    embedding vector(768)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_id ON Transactions(Transactions_Id);
CREATE INDEX IF NOT EXISTS idx_transfers_id ON Transactions(Transfers_Id) WHERE Transfers_Id IS NOT NULL;

-- Πίνακας Splits (Εδώ γίνεται η ανάλυση κατηγοριών)
CREATE TABLE Splits (
    Splits_Id SERIAL PRIMARY KEY,
    Transactions_Id INTEGER NOT NULL REFERENCES Transactions(Transactions_Id) ON DELETE CASCADE,
    Categories_Id INTEGER REFERENCES Categories(Categories_Id),
    Amount NUMERIC(28, 18),
    Memo TEXT,
    embedding vector(768)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_splits_id ON Splits(Splits_Id);
CREATE INDEX IF NOT EXISTS idx_splits_tx_id ON Splits(Transactions_Id);


CREATE OR REPLACE FUNCTION update_accounts_balance()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        UPDATE Accounts SET Accounts_Balance = Accounts_Balance + NEW.Total_Amount 
        WHERE Accounts_Id = NEW.Accounts_Id;
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE Accounts SET Accounts_Balance = Accounts_Balance - OLD.Total_Amount 
        WHERE Accounts_Id = OLD.Accounts_Id;
    ELSIF (TG_OP = 'UPDATE') THEN
        -- 1. Αφαιρούμε το παλιό ποσό από τον ΠΑΛΙΟ λογαριασμό
        UPDATE Accounts SET Accounts_Balance = Accounts_Balance - OLD.Total_Amount 
        WHERE Accounts_Id = OLD.Accounts_Id;
        -- 2. Προσθέτουμε το νέο ποσό στον ΝΕΟ λογαριασμό
        UPDATE Accounts SET Accounts_Balance = Accounts_Balance + NEW.Total_Amount 
        WHERE Accounts_Id = NEW.Accounts_Id;		
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: public.update_accounts_balance_with_transfer()

-- DROP FUNCTION IF EXISTS public.update_accounts_balance_with_transfer();

CREATE OR REPLACE FUNCTION public.update_accounts_balance_with_transfer()
    RETURNS trigger AS $$
BEGIN
    -- INSERT LOGIC
    IF (TG_OP = 'INSERT') THEN
        UPDATE Accounts SET Accounts_Balance = Accounts_Balance + NEW.Total_Amount WHERE Accounts_Id = NEW.Accounts_Id;
        IF NEW.Accounts_Id_Target IS NOT NULL THEN
            -- Χρησιμοποιούμε το Total_Amount_Target αν υπάρχει (για FX), αλλιώς το αντίστροφο του Total_Amount
            UPDATE Accounts SET Accounts_Balance = Accounts_Balance + COALESCE(NEW.Total_Amount_Target, -NEW.Total_Amount) 
            WHERE Accounts_Id = NEW.Accounts_Id_Target;
        END IF;

    -- DELETE LOGIC
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE Accounts SET Accounts_Balance = Accounts_Balance - OLD.Total_Amount WHERE Accounts_Id = OLD.Accounts_Id;
        IF OLD.Accounts_Id_Target IS NOT NULL THEN
            UPDATE Accounts SET Accounts_Balance = Accounts_Balance - COALESCE(OLD.Total_Amount_Target, -OLD.Total_Amount) 
            WHERE Accounts_Id = OLD.Accounts_Id_Target;
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER trg_update_balance
AFTER INSERT OR UPDATE OR DELETE ON Transactions
FOR EACH ROW EXECUTE FUNCTION update_accounts_balance_with_transfer();


CREATE TABLE Investments (
    Investments_Id SERIAL PRIMARY KEY,
    Accounts_Id INTEGER NOT NULL REFERENCES Accounts(Accounts_Id),
    Securities_Id INTEGER REFERENCES Securities(Securities_Id),
    Date DATE NOT NULL,
    Action Investments_Action NOT NULL,
    Quantity NUMERIC(28, 18),         -- Αριθμός μετοχών
    Price_Per_Share NUMERIC(20, 8),
    Commission NUMERIC(20, 8) DEFAULT 0,
    Total_Amount NUMERIC(28, 18),      -- Συνολικό ποσό μετρητών που κινήθηκε
    Description TEXT,
    embedding vector(768)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_investments_id ON Investments(Investments_Id);

-- FUNCTION: public.update_holdings_from_investments()

-- DROP FUNCTION IF EXISTS public.update_holdings_from_investments();

CREATE OR REPLACE FUNCTION public.update_holdings_from_investments()
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
            Investments_Id,
            Action,
            Quantity,
            Price_Per_Share,
            -- Ιστορικός Μέσος Όρος (Simple Average)
            AVG(Price_Per_Share) FILTER (WHERE Action IN ('Buy', 'Reinvest', 'ShrIn')) 
                OVER (PARTITION BY Accounts_Id, Securities_Id) as simple_avg_cost,
            -- Σωρευτικά FIFO
            SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity ELSE 0 END) 
                OVER (PARTITION BY Accounts_Id, Securities_Id ORDER BY Date, Investments_Id) as running_buys,
            SUM(CASE WHEN Action IN ('Sell', 'ShrOut') THEN Quantity ELSE 0 END) 
                OVER (PARTITION BY Accounts_Id, Securities_Id ORDER BY Date, Investments_Id) as running_sells,
            SUM(CASE WHEN Action IN ('Buy', 'Reinvest', 'ShrIn') THEN Quantity ELSE 0 END) 
                OVER (PARTITION BY Accounts_Id, Securities_Id) as total_buys,
            SUM(CASE WHEN Action IN ('Sell', 'ShrOut') THEN Quantity ELSE 0 END) 
                OVER (PARTITION BY Accounts_Id, Securities_Id) as total_sells
        FROM Investments
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
        MAX(simple_avg_cost),
        CASE WHEN ABS(SUM(remaining_qty)) > 0 THEN SUM(ABS(remaining_qty) * Price_Per_Share) / SUM(ABS(remaining_qty)) ELSE 0 END,
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

ALTER FUNCTION public.update_holdings_from_investments()
    OWNER TO admin;



-- Ιστορικό τιμών για Μετοχές, ETFs, Crypto, PF Units
CREATE TABLE Historical_Prices (
    Securities_Id INTEGER REFERENCES Securities(Securities_Id) ON DELETE CASCADE,
    Date DATE NOT NULL,
    Close NUMERIC(20, 8) NOT NULL,
    High NUMERIC(20, 8),
    Low NUMERIC(20, 8),
    Volume BIGINT,
    embedding vector(768),
    PRIMARY KEY (Securities_Id, Date)
);
ALTER TABLE Historical_Prices ADD UNIQUE (Securities_Id, Date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_price_id ON Historical_Prices(Securities_Id, Date);

-- Ιστορικό ισοτιμιών (FX Rates)
CREATE TABLE Historical_FX (
    Currencies_Id_1 INTEGER REFERENCES Currencies(Currencies_Id),   -- π.χ. GBP
    Currencies_Id_2 INTEGER REFERENCES Currencies(Currencies_Id), -- π.χ. EUR
    Date DATE NOT NULL,
    FX_Rate NUMERIC(18, 10) NOT NULL,
    embedding vector(768),
    PRIMARY KEY (Currencies_Id_1, Currencies_Id_2, Date)
);
ALTER TABLE Historical_FX ADD UNIQUE (Currencies_Id_1, Currencies_Id_2, Date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fxrate_id ON Historical_FX(Currencies_Id_1, Currencies_Id_2, Date);


ALTER DATABASE "Finance" REFRESH COLLATION VERSION;


-- Transactions: the historical balance reconstruction queries heavily
CREATE INDEX idx_transactions_accounts_date ON Transactions(Accounts_Id, Date DESC);

-- Investments: used in correlated subqueries for historical quantity reconstruction  
CREATE INDEX idx_investments_accsec_date ON Investments(Accounts_Id, Securities_Id, Date DESC);

-- Historical_FX: scanned per-transaction in correlated subqueries
CREATE INDEX idx_fx_currency_date ON Historical_FX(Currencies_Id_1, Date DESC);

-- Historical_Prices: same pattern
CREATE INDEX idx_prices_sec_date ON Historical_Prices(Securities_Id, Date DESC);

-- Splits: the income/expense queries join this heavily
CREATE INDEX idx_splits_category ON Splits(Categories_Id) WHERE Categories_Id IS NOT NULL;

-- Investments: filtered by action type in many queries
CREATE INDEX idx_investments_action ON Investments(Action);