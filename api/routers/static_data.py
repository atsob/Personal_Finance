"""Static Data API endpoints: institutions, categories, payees, accounts."""
from fastapi import APIRouter, Query
from typing import Optional
import pandas as pd
from database.connection import get_db

router = APIRouter()


def _df(df: pd.DataFrame) -> list:
    df = df.copy()
    for col in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
        df[col] = df[col].astype(str)
    return df.where(pd.notnull(df), None).to_dict(orient="records")


@router.get("/institutions")
def get_institutions(search: Optional[str] = Query(None)):
    clause = "AND (LOWER(Institutions_Name) LIKE %(s)s OR LOWER(Institutions_Type) LIKE %(s)s)" if search else ""
    params: dict = {}
    if search:
        params["s"] = f"%{search.lower()}%"
    with get_db() as conn:
        df = pd.read_sql(f"""
            SELECT Institutions_Id AS id, Institutions_Name AS name,
                   Institutions_Type AS type, BIC_Code AS bic,
                   Moodys AS moodys, S_P AS sp, Fitch AS fitch,
                   Contact AS contact, Phone AS phone,
                   Email AS email, Website AS website, Notes AS notes
            FROM Institutions
            WHERE 1=1 {clause}
            ORDER BY Institutions_Name ASC
        """, conn, params=params if params else None)
    return _df(df)


@router.get("/categories")
def get_categories(search: Optional[str] = Query(None)):
    clause = "AND LOWER(ch.Full_Path) LIKE %(s)s" if search else ""
    params: dict = {}
    if search:
        params["s"] = f"%{search.lower()}%"
    with get_db() as conn:
        df = pd.read_sql(f"""
            WITH RECURSIVE CategoryHierarchy AS (
                SELECT Categories_Id, Categories_Name::TEXT AS Full_Path,
                       Categories_Type::TEXT AS Categories_Type,
                       Categories_Id_Parent, 0 AS Level
                FROM Categories WHERE Categories_Id_Parent IS NULL
                UNION ALL
                SELECT c.Categories_Id, ch.Full_Path || ' : ' || c.Categories_Name,
                       c.Categories_Type::TEXT, c.Categories_Id_Parent, ch.Level + 1
                FROM Categories c JOIN CategoryHierarchy ch ON c.Categories_Id_Parent = ch.Categories_Id
            ),
            SplitCounts AS (
                SELECT Categories_Id, COUNT(*) AS cnt FROM Splits GROUP BY Categories_Id
            )
            SELECT ch.Categories_Id AS id, ch.Full_Path AS full_path,
                   ch.Categories_Type AS type, ch.Level AS level,
                   COALESCE(sc.cnt, 0) AS transactions_count
            FROM CategoryHierarchy ch
            LEFT JOIN SplitCounts sc ON sc.Categories_Id = ch.Categories_Id
            WHERE 1=1 {clause}
            ORDER BY ch.Full_Path ASC
        """, conn, params=params if params else None)
    return _df(df)


@router.get("/payees")
def get_payees(search: Optional[str] = Query(None)):
    clause = "AND LOWER(p.Payees_Name) LIKE %(s)s" if search else ""
    params: dict = {}
    if search:
        params["s"] = f"%{search.lower()}%"
    with get_db() as conn:
        df = pd.read_sql(f"""
            WITH RECURSIVE ch AS (
                SELECT Categories_Id, Categories_Name::TEXT AS full_path
                FROM Categories WHERE Categories_Id_Parent IS NULL
                UNION ALL
                SELECT c.Categories_Id, ch.full_path || ' : ' || c.Categories_Name
                FROM Categories c JOIN ch ON c.Categories_Id_Parent = ch.Categories_Id
            )
            SELECT p.Payees_Id AS id, p.Payees_Name AS name,
                   ch.full_path AS default_category,
                   COUNT(t.Transactions_Id) AS transactions_count,
                   MAX(t.Date) AS last_transaction
            FROM Payees p
            LEFT JOIN Transactions t ON t.Payees_Id = p.Payees_Id
            LEFT JOIN ch ON ch.Categories_Id = p.Categories_Id
            WHERE 1=1 {clause}
            GROUP BY p.Payees_Id, p.Payees_Name, ch.full_path
            ORDER BY transactions_count DESC, p.Payees_Name ASC
        """, conn, params=params if params else None)
    return _df(df)


@router.get("/accounts-master")
def get_accounts_master(search: Optional[str] = Query(None)):
    """Full account master data (all fields)."""
    clause = "AND (LOWER(a.Accounts_Name) LIKE %(s)s OR LOWER(a.Accounts_Type) LIKE %(s)s)" if search else ""
    params: dict = {}
    if search:
        params["s"] = f"%{search.lower()}%"
    with get_db() as conn:
        df = pd.read_sql(f"""
            SELECT a.Accounts_Id AS id, a.Accounts_Name AS name,
                   a.Accounts_Type AS type, a.Accounts_Balance AS balance,
                   a.Is_Active AS is_active, c.Currencies_ShortName AS currency,
                   i.Institutions_Name AS institution,
                   a.Accounts_Number AS account_number,
                   a.Notes AS notes
            FROM Accounts a
            JOIN Currencies c ON a.Currencies_Id = c.Currencies_Id
            LEFT JOIN Institutions i ON a.Institutions_Id = i.Institutions_Id
            WHERE 1=1 {clause}
            ORDER BY a.Accounts_Type, a.Accounts_Name
        """, conn, params=params if params else None)
    return _df(df)
