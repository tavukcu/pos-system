"""
Karpin (SQL Server) -> Railway (PostgreSQL) otomatik senkronizasyon.
Belirli araliklarla yeni satislari, odemeleri ve stok hareketlerini aktarir.
Kullanim: python sync_to_railway.py

Windows Task Scheduler ile her dakika calistirilir.
Zaten calisan bir instance varsa otomatik olarak cikar (lock dosyasi).
"""
import pyodbc
import requests
import json
import time
import logging
import os
import sys
import atexit
from datetime import datetime, date
from decimal import Decimal

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Lock dosyasi - ayni anda birden fazla instance calismasini engelle
LOCK_FILE = os.path.join(SCRIPT_DIR, 'sync.lock')

def acquire_lock():
    """Cift instance calismasini engelle."""
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, 'r') as f:
                old_pid = int(f.read().strip())
            # PID hala calisiyor mu kontrol et
            import ctypes
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000, False, old_pid)  # PROCESS_QUERY_LIMITED_INFORMATION
            if handle:
                kernel32.CloseHandle(handle)
                # Eski process hala calisiyor
                return False
        except (ValueError, OSError, AttributeError):
            pass  # Lock dosyasi bozuk veya PID kontrol edilemedi, devam et
    # Lock dosyasini olustur
    with open(LOCK_FILE, 'w') as f:
        f.write(str(os.getpid()))
    return True

def release_lock():
    """Lock dosyasini sil."""
    try:
        if os.path.exists(LOCK_FILE):
            with open(LOCK_FILE, 'r') as f:
                pid = int(f.read().strip())
            if pid == os.getpid():
                os.remove(LOCK_FILE)
    except:
        pass

# Log dosyasi ayarla
LOG_FILE = os.path.join(SCRIPT_DIR, 'sync.log')

# Log dosyasi cok buyurse truncate et (5MB)
try:
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 5 * 1024 * 1024:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.writelines(lines[-500:])  # Son 500 satiri tut
except:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.info

API_URL = "https://pos-app-production-f14d.up.railway.app"
SECRET = "pos-migrate-2024"
BATCH_SIZE = 500
SYNC_INTERVAL = 15  # 15 saniye

mssql_conn_str = 'DRIVER={SQL Server};SERVER=(local);DATABASE=BUSINESS2023;UID=sa;PWD=Ceddan1234;'

# Baglanti havuzu - her seferinde yeni baglanti acmak yerine yeniden kullan
_mssql_conn = None

def reset_mssql_conn():
    """SQL Server baglantisini sifirla."""
    global _mssql_conn
    try:
        if _mssql_conn:
            _mssql_conn.close()
    except:
        pass
    _mssql_conn = None

def get_mssql():
    """SQL Server baglantisi al, kopuksa yeniden baglan."""
    global _mssql_conn
    if _mssql_conn is not None:
        try:
            _mssql_conn.cursor().execute("SELECT 1")
            return _mssql_conn
        except:
            try:
                _mssql_conn.close()
            except:
                pass
            _mssql_conn = None
    _mssql_conn = pyodbc.connect(mssql_conn_str, timeout=10)
    return _mssql_conn

def serialize(val):
    if val is None:
        return None
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float, Decimal)):
        return float(val)
    return str(val)

def get_max_ids():
    """Railway'deki her tablonun max ID'sini al."""
    try:
        r = requests.post(f"{API_URL}/api/sync/max-id", json={'secret': SECRET}, timeout=30)
        r.raise_for_status()
        data = r.json()
        # tbStokFisiDetayi ve tbMusteri integer olmali
        for key in ('tbStokFisiDetayi', 'tbMusteri'):
            if key in data and data[key] is not None:
                try:
                    data[key] = int(float(data[key]))
                except (ValueError, TypeError):
                    data[key] = 0
        return data
    except requests.exceptions.ConnectionError as e:
        log(f"  Railway baglanti hatasi: {e}")
        return None
    except requests.exceptions.Timeout:
        log(f"  Railway timeout")
        return None
    except Exception as e:
        log(f"  Max ID alinamadi: {e}")
        return None

def upload_rows(table, cols, rows):
    """Satirlari Railway'e yukle."""
    total = len(rows)
    uploaded = 0
    for i in range(0, total, BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        data = {
            'secret': SECRET,
            'table': table,
            'cols': cols,
            'rows': [[serialize(v) for v in row] for row in batch]
        }
        try:
            r = requests.post(f"{API_URL}/api/migrate/data", json=data, timeout=120)
            r.raise_for_status()
            result = r.json()
            inserted = result.get('inserted', 0)
            uploaded += inserted
            if result.get('errors'):
                for err in result['errors']:
                    log(f"  DB HATA {table}: {err}")
            if result.get('error'):
                log(f"  UYARI {table} batch {i}: {result['error']}")
        except requests.exceptions.Timeout:
            log(f"  TIMEOUT {table} batch {i} - sonraki sync'te tekrar denenecek")
        except Exception as e:
            log(f"  HATA {table} batch {i}: {e}")
    return uploaded

def sync_table(conn, table, id_col, max_id, sql_template):
    """Bir tablodaki yeni kayitlari senkronize et."""
    cursor = conn.cursor()
    cursor.execute(sql_template, (max_id,))
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    if not rows:
        return 0
    log(f"  {table}: {len(rows)} yeni kayit bulundu, aktariliyor...")
    uploaded = upload_rows(table, cols, rows)
    log(f"  {table}: {uploaded}/{len(rows)} kayit aktarildi")
    return uploaded

def do_sync():
    """Tek bir senkronizasyon dongusunu calistir.

    Karpin POS once satisi 0 tutarla kaydeder, sonra gunceller.
    Bu yuzden sadece yeni kayitlari degil, son 10 dakikadaki kayitlari da
    tekrar gondeririz. UPSERT sayesinde mevcut kayitlar guncellenir.
    """
    max_ids = get_max_ids()
    if max_ids is None:
        return

    conn = get_mssql()
    total = 0

    # Alisveris (satislar) - tarih bazli, 10 dk geriye bak
    max_dt = max_ids.get('tbAlisVeris') or '2000-01-01'
    total += sync_table(conn, 'tbAlisVeris', 'dteKayitTarihi', max_dt,
        "SELECT nAlisverisID, sFisTipi, dteFaturaTarihi, nGirisCikis, lFaturaNo, "
        "nMusteriID, sMagaza, sKasiyerRumuzu, sAlisverisYapanAdi, sAlisverisYapanSoyadi, "
        "lToplamMiktar, lMalBedeli, lMalIskontoTutari, nDipIskontoYuzdesi, lDipIskontoTutari, "
        "nKdvOrani1, lKdvMatrahi1, lKdv1, nKdvOrani2, lKdvMatrahi2, lKdv2, "
        "nKdvOrani3, lKdvMatrahi3, lKdv3, nKdvOrani4, lKdvMatrahi4, lKdv4, "
        "nKdvOrani5, lKdvMatrahi5, lKdv5, lPesinat, nVadeFarkiYuzdesi, "
        "nVadeKdvOrani, lVadeKdvMatrahi, lVadeKdv, lVadeFarki, lNetTutar, "
        "sHareketTipi, bMuhasebeyeIslendimi, sKullaniciAdi, dteKayitTarihi "
        "FROM tbAlisVeris WHERE dteKayitTarihi >= DATEADD(MINUTE, -10, ?) "
        "AND lNetTutar < 10000000"
    )

    # Odemeler - tarih bazli, 10 dk geriye bak
    max_dt = max_ids.get('tbOdeme') or '2000-01-01'
    total += sync_table(conn, 'tbOdeme', 'dteKayitTarihi', max_dt,
        "SELECT nOdemeID, nAlisverisID, sOdemeSekli, nOdemeKodu, sKasiyerRumuzu, "
        "dteOdemeTarihi, dteValorTarihi, lOdemeTutar, sDovizCinsi, lDovizTutar, "
        "lMakbuzNo, lOdemeNo, nTaksitID, nIadeAlisverisID, bMuhasebeyeIslendimi, "
        "nKasaNo, sKullaniciAdi, dteKayitTarihi, sMagaza FROM tbOdeme "
        "WHERE dteKayitTarihi >= DATEADD(MINUTE, -10, ?)"
    )

    # Stok hareketleri - integer bazli (ID bazli, geriye bakma gerekli degil)
    max_islem = max_ids.get('tbStokFisiDetayi', 0) or 0
    total += sync_table(conn, 'tbStokFisiDetayi', 'nIslemID', max_islem,
        "SELECT nIslemID, nStokID, dteIslemTarihi, nFirmaID, nMusteriID, sFisTipi, "
        "dteFisTarihi, lFisNo, nGirisCikis, sDepo, lReyonFisNo, sStokIslem, "
        "sKasiyerRumuzu, sSaticiRumuzu, sOdemeKodu, dteIrsaliyeTarihi, lIrsaliyeNo, "
        "lGirisMiktar1, lGirisMiktar2, lGirisFiyat, lGirisTutar, "
        "lCikisMiktar1, lCikisMiktar2, lCikisFiyat, lCikisTutar, "
        "sFiyatTipi, lBrutFiyat, lBrutTutar, lMaliyetFiyat, lMaliyetTutar, "
        "lIlaveMaliyetTutar, nIskontoYuzdesi, lIskontoTutari, sDovizCinsi, lDovizFiyat, "
        "nSiparisID, nReceteNo, nTransferID, sTransferDepo, nKdvOrani, nHesapID, "
        "sAciklama, sHareketTipi, bMuhasebeyeIslendimi, sKullaniciAdi, dteKayitTarihi, "
        "nAlisverisID, nStokFisiID, nIrsaliyeFisiID FROM tbStokFisiDetayi WHERE nIslemID > ?"
    )

    # Musteriler (yeni musteriler)
    max_musteri = max_ids.get('tbMusteri', 0) or 0
    total += sync_table(conn, 'tbMusteri', 'nMusteriID', max_musteri,
        "SELECT nMusteriID, sAdi, sSoyadi, sGSM AS sTelefon1, sEvIl AS sIl "
        "FROM tbMusteri WHERE nMusteriID > ?"
    )

    if total > 0:
        log(f"  Toplam {total} yeni kayit aktarildi.")

if __name__ == '__main__':
    if not acquire_lock():
        print("Sync zaten calisiyor, cikiliyor.")
        sys.exit(0)
    atexit.register(release_lock)

    log("=" * 50)
    log("Karpin -> Railway Otomatik Senkronizasyon baslatildi")
    log(f"PID: {os.getpid()} | Her {SYNC_INTERVAL} saniyede bir calisacak")
    log("=" * 50)

    fail_count = 0
    while True:
        try:
            do_sync()
            fail_count = 0
        except pyodbc.Error as e:
            fail_count += 1
            log(f"  SQL Server hatasi: {e}")
            # Baglanti bozulmus olabilir, sifirlayalim
            reset_mssql_conn()
            if fail_count >= 5:
                log(f"  {fail_count} ardisik hata, 60 saniye bekleniyor...")
                time.sleep(60)
        except Exception as e:
            fail_count += 1
            log(f"  HATA: {e}")
            if fail_count >= 5:
                log(f"  {fail_count} ardisik hata, 60 saniye bekleniyor...")
                time.sleep(60)
        time.sleep(SYNC_INTERVAL)
