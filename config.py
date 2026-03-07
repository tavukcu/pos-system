import os

DB_MODE = os.environ.get('DB_MODE', 'sqlserver')  # 'sqlserver' or 'postgres'

def adapt_sql(sql):
    """SQL Server syntax'ini PostgreSQL'e cevir."""
    if DB_MODE != 'postgres':
        return sql
    import re
    # TOP N -> LIMIT N
    m = re.search(r'\bSELECT\s+TOP\s+(\d+)\s+', sql, re.IGNORECASE)
    if m:
        n = m.group(1)
        sql = re.sub(r'\bTOP\s+\d+\s+', '', sql, count=1, flags=re.IGNORECASE)
        sql = sql.rstrip().rstrip(';') + f' LIMIT {n}'
    # ISNULL -> COALESCE
    sql = re.sub(r'\bISNULL\b', 'COALESCE', sql, flags=re.IGNORECASE)
    # RTRIM
    sql = re.sub(r'\bRTRIM\b', 'RTRIM', sql, flags=re.IGNORECASE)
    # CAST(x AS DATE) -> x::date
    sql = re.sub(r'CAST\((\w+)\s+AS\s+DATE\)', r'\1::date', sql, flags=re.IGNORECASE)
    # GETDATE() -> NOW()
    sql = re.sub(r'\bGETDATE\(\)', 'NOW()', sql, flags=re.IGNORECASE)
    # DATEADD(DAY, n, x) -> x + interval 'n days'
    def dateadd_replace(m):
        unit = m.group(1).strip()
        val = m.group(2).strip()
        expr = m.group(3).strip()
        return f"({expr} + ({val}) * interval '1 {unit}')"
    sql = re.sub(r'DATEADD\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)', dateadd_replace, sql, flags=re.IGNORECASE)
    # MONTH(x) -> EXTRACT(MONTH FROM x)
    sql = re.sub(r'\bMONTH\(([^)]+)\)', r'EXTRACT(MONTH FROM \1)::int', sql, flags=re.IGNORECASE)
    # YEAR(x) -> EXTRACT(YEAR FROM x)
    sql = re.sub(r'\bYEAR\(([^)]+)\)', r'EXTRACT(YEAR FROM \1)::int', sql, flags=re.IGNORECASE)
    # ? -> %s
    sql = sql.replace('?', '%s')
    return sql


if DB_MODE == 'postgres':
    import psycopg2
    import psycopg2.extras

    DATABASE_URL = os.environ.get('DATABASE_URL', '')

    def get_connection():
        return psycopg2.connect(DATABASE_URL)

    def query(sql, params=None):
        sql = adapt_sql(sql)
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql, params or [])
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows

    def execute(sql, params=None):
        sql = adapt_sql(sql)
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or [])
        conn.commit()
        conn.close()

else:
    import pyodbc

    DB_CONFIG = {
        'driver': '{SQL Server}',
        'server': '(local)',
        'database': 'BUSINESS2023',
        'uid': 'sa',
        'pwd': 'Ceddan1234',
    }

    def get_connection():
        conn_str = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['uid']};"
            f"PWD={DB_CONFIG['pwd']};"
        )
        return pyodbc.connect(conn_str)

    def query(sql, params=None):
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or [])
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        return rows

    def execute(sql, params=None):
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or [])
        conn.commit()
        conn.close()
