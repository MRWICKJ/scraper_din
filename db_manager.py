import sqlite3
import re
from pathlib import Path

MASTER_DB_PATH = Path(__file__).parent / "all_companies_master.db"

def sanitize_name(name: str) -> str:
    if not name:
        return "Unknown"
    clean = re.sub(r'[^\w\.-]', '_', str(name).strip())
    return re.sub(r'_+', '_', clean)

def _to_float_money(v):
    """Normalize '₹10,00,000' / 'Rs. 50000' / 50000.0 -> float. (#8)"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s or s.lower() in ("unknown", "none", "-", "null", "nan"):
        return None
    s = re.sub(r"(rs\.?|inr|₹|\$)", "", s, flags=re.I).strip()
    m = re.search(r"[\d,]+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None

def _get_basic(basic: dict, *candidates):
    """Case-insensitive key lookup across parser variants. (#9)"""
    if not basic:
        return None
    for cand in candidates:
        if cand in basic and basic[cand] not in (None, ""):
            return basic[cand]
    lower_map = {str(k).strip().lower(): v for k, v in basic.items()}
    for cand in candidates:
        v = lower_map.get(str(cand).strip().lower())
        if v not in (None, ""):
            return v
    return None

def init_db(db_path: Path = MASTER_DB_PATH):
    """Initializes clean structured tables inside all_companies_master.db."""
    conn = sqlite3.connect(db_path, timeout=60.0)
    cursor = conn.cursor()

    cursor.execute('PRAGMA journal_mode=WAL;')

    # 1. Main Structured Company Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            cin TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            pincode TEXT NOT NULL,
            post_office_name TEXT,
            district TEXT,
            sub_district TEXT,
            status TEXT,
            roc TEXT,
            incorporation_date TEXT,
            authorized_capital REAL,
            paid_up_capital REAL,
            email TEXT,
            phone TEXT,
            website TEXT,
            registered_address TEXT,
            source_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 2. Directors Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS directors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cin TEXT NOT NULL,
            din TEXT,
            name TEXT NOT NULL,
            designation TEXT,
            appointment_date TEXT,
            is_active BOOLEAN DEFAULT 1,
            FOREIGN KEY (cin) REFERENCES companies (cin) ON DELETE CASCADE
        )
    ''')

    # 3. Charges Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS charges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cin TEXT NOT NULL,
            charge_id TEXT,
            creation_date TEXT,
            modification_date TEXT,
            closure_date TEXT,
            amount REAL,
            charge_holder TEXT,
            status TEXT,
            FOREIGN KEY (cin) REFERENCES companies (cin) ON DELETE CASCADE
        )
    ''')

    # 4. Scrape Source & Audit Logs
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scrape_logs (
            cin TEXT PRIMARY KEY,
            primary_source TEXT,
            status TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Migration for #12: error column (safe if table already exists)
    cols = [r[1] for r in cursor.execute("PRAGMA table_info(scrape_logs)").fetchall()]
    if "error" not in cols:
        cursor.execute("ALTER TABLE scrape_logs ADD COLUMN error TEXT DEFAULT ''")

    # 5. File-level resume ledger: skip fully-processed CSVs on next run.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_progress (
            csv_path TEXT PRIMARY KEY,
            mtime REAL,
            size INTEGER,
            total_rows INTEGER,
            status TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_companies_pincode ON companies(pincode)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_directors_din ON directors(din)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_charges_cin ON charges(cin)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_scrape_logs_cin ON scrape_logs(cin)')

    conn.commit()
    conn.close()

def get_file_progress(db_path: Path, csv_path: str):
    conn = sqlite3.connect(db_path, timeout=60.0)
    try:
        cur = conn.cursor()
        cur.execute("SELECT mtime, size, total_rows, status FROM file_progress WHERE csv_path = ?", (csv_path,))
        return cur.fetchone()
    finally:
        conn.close()


def set_file_progress(db_path: Path, csv_path: str, mtime: float, size: int, total_rows: int, status: str):
    conn = sqlite3.connect(db_path, timeout=60.0)
    try:
        conn.execute(
            """INSERT INTO file_progress (csv_path, mtime, size, total_rows, status, updated_at)
               VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(csv_path) DO UPDATE SET
                 mtime=excluded.mtime, size=excluded.size, total_rows=excluded.total_rows,
                 status=excluded.status, updated_at=CURRENT_TIMESTAMP""",
            (csv_path, mtime, size, total_rows, status),
        )
        conn.commit()
    finally:
        conn.close()


def save_to_master_sqlite(data: dict):
    # NOTE: no init_db() here (was per-save contention, #5). Caller must init once.
    conn = sqlite3.connect(MASTER_DB_PATH, timeout=60.0)
    cursor = conn.cursor()
    try:
        basic = data.get("basic_info", {})

        cursor.execute('''
            INSERT INTO companies (
                cin, company_name, pincode, post_office_name, district, sub_district,
                status, roc, incorporation_date, authorized_capital,
                paid_up_capital, email, phone, website,
                registered_address, source_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cin) DO UPDATE SET
                company_name=COALESCE(excluded.company_name, companies.company_name),
                pincode=COALESCE(excluded.pincode, companies.pincode),
                post_office_name=COALESCE(excluded.post_office_name, companies.post_office_name),
                district=COALESCE(excluded.district, companies.district),
                sub_district=COALESCE(excluded.sub_district, companies.sub_district),
                status=COALESCE(excluded.status, companies.status),
                roc=COALESCE(excluded.roc, companies.roc),
                incorporation_date=COALESCE(excluded.incorporation_date, companies.incorporation_date),
                authorized_capital=COALESCE(excluded.authorized_capital, companies.authorized_capital),
                paid_up_capital=COALESCE(excluded.paid_up_capital, companies.paid_up_capital),
                email=COALESCE(excluded.email, companies.email),
                phone=COALESCE(excluded.phone, companies.phone),
                website=COALESCE(excluded.website, companies.website),
                registered_address=COALESCE(excluded.registered_address, companies.registered_address),
                source_url=COALESCE(excluded.source_url, companies.source_url)
        ''', (
            data.get("cin"),
            data.get("company_name"),
            data.get("pincode"),
            data.get("post_office_name"),
            data.get("district"),
            data.get("sub_district"),
            _get_basic(basic, "status", "Company Status", "company_status"),
            _get_basic(basic, "roc", "RoC", "ROC", "Registrar of Companies"),
            _get_basic(basic, "incorporation_date", "Date of Incorporation", "Incorporation Date", "date_of_incorporation"),
            _to_float_money(_get_basic(basic, "authorized_capital", "Authorized Capital", "Authorised Capital")),
            _to_float_money(_get_basic(basic, "paid_up_capital", "Paid-Up Capital", "Paid Up Capital", "PaidUp Capital")),
            _get_basic(basic, "email", "Email"),
            _get_basic(basic, "phone", "Phone"),
            _get_basic(basic, "website", "Website"),
            _get_basic(basic, "registered_address", "Registered Address"),
            data.get("source_url")
        ))

        directors = data.get("directors", [])
        if directors:
            cursor.execute('DELETE FROM directors WHERE cin = ?', (data.get("cin"),))
            director_rows = [
                (
                    data.get("cin"),
                    d.get("din") or d.get("DIN") or None,
                    d.get("name") or d.get("Name") or "Unknown",
                    d.get("designation") or d.get("Designation") or "Director",
                    d.get("appointment_date") or d.get("Appointment Date"),
                    d.get("is_active", True)
                )
                for d in directors
            ]
            cursor.executemany('''
                INSERT INTO directors (cin, din, name, designation, appointment_date, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', director_rows)

        charges = data.get("charges", [])
        if charges:
            cursor.execute('DELETE FROM charges WHERE cin = ?', (data.get("cin"),))
            charge_rows = [
                (
                    data.get("cin"),
                    c.get("charge_id"),
                    c.get("creation_date"),
                    c.get("modification_date"),
                    c.get("closure_date"),
                    _to_float_money(c.get("amount")),
                    c.get("charge_holder"),
                    c.get("status")
                )
                for c in charges
            ]
            cursor.executemany('''
                INSERT INTO charges (cin, charge_id, creation_date, modification_date, closure_date, amount, charge_holder, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', charge_rows)

        conn.commit()
    finally:
        conn.close()
