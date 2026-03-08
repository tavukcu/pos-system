"""
Karpin (SQL Server) -> Railway (PostgreSQL) otomatik senkronizasyon.
Belirli araliklarla yeni satislari, odemeleri ve stok hareketlerini aktarir.
Kullanim: python sync_to_railway.py
"""
import pyodbc
import requests
import json
import time
from datetime import datetime, date

API_URL = "https://pos-app-production-f14d.up.railway.app"
SECRET = "pos-migrate-2024"
BATCH_SIZE = 500
SYNC_INTERVAL = 300  # 5 dakika (saniye)

mssql_conn_str = 'DRIVER={SQL Server};SERVER=(local);DATABASE=BUSINESS2023;UID=sa;PWD=Ceddan1234;'

def get_mssql():
    return pyodbc.connect(mssql_conn_str)

def serialize(val):
    if val is None:
        return None
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, bool):
        return val
    return float(val) if isinstance(val, (int, float)) else str(val)

def get_max_ids():
    """Railway'deki her tablonun max ID'sini al."""
    try:
        r = requests.post(f"{API_URL}/api/sync/max-id", json={'secret': SECRET}, timeout=30)
        return r.json()
    except Exception as e:
        print(f"  Max ID alinamadi: {e}")
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
            result = r.json()
            uploaded += result.get('inserted', 0)
        except Exception as e:
            print(f"  HATA batch {i}: {e}")
    return uploaded

def sync_table(conn, table, id_col, max_id, sql_template):
    """Bir tablodaki yeni kayitlari senkronize et."""
    cursor = conn.cursor()
    cursor.execute(sql_template, (max_id,))
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    if not rows:
        return 0
    uploaded = upload_rows(table, cols, rows)
    print(f"  {table}: {uploaded} yeni kayit aktarildi")
    return uploaded

def do_sync():
    """Tek bir senkronizasyon dongusunu calistir."""
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Senkronizasyon basliyor...")

    max_ids = get_max_ids()
    if max_ids is None:
        print("  Railway'e ulasilamadi, sonraki dongude tekrar denenecek.")
        return

    conn = get_mssql()
    total = 0

    # Alisveris (satislar)
    total += sync_table(conn, 'tbAlisVeris', 'nAlisverisID', max_ids.get('tbAlisVeris', 0),
        "SELECT nAlisverisID, sFisTipi, dteFaturaTarihi, nGirisCikis, lFaturaNo, "
        "nMusteriID, sMagaza, sKasiyerRumuzu, sAlisverisYapanAdi, sAlisverisYapanSoyadi, "
        "lToplamMiktar, lMalBedeli, lMalIskontoTutari, nDipIskontoYuzdesi, lDipIskontoTutari, "
        "nKdvOrani1, lKdvMatrahi1, lKdv1, nKdvOrani2, lKdvMatrahi2, lKdv2, "
        "nKdvOrani3, lKdvMatrahi3, lKdv3, nKdvOrani4, lKdvMatrahi4, lKdv4, "
        "nKdvOrani5, lKdvMatrahi5, lKdv5, lPesinat, nVadeFarkiYuzdesi, "
        "nVadeKdvOrani, lVadeKdvMatrahi, lVadeKdv, lVadeFarki, lNetTutar, "
        "sHareketTipi, bMuhasebeyeIslendimi, sKullaniciAdi, dteKayitTarihi "
        "FROM tbAlisVeris WHERE nAlisverisID > ? AND lNetTutar < 10000000"
    )

    # Odemeler
    total += sync_table(conn, 'tbOdeme', 'nOdemeID', max_ids.get('tbOdeme', 0),
        "SELECT nOdemeID, nAlisverisID, sOdemeSekli, nOdemeKodu, sKasiyerRumuzu, "
        "dteOdemeTarihi, dteValorTarihi, lOdemeTutar, sDovizCinsi, lDovizTutar, "
        "lMakbuzNo, lOdemeNo, nTaksitID, nIadeAlisverisID, bMuhasebeyeIslendimi, "
        "nKasaNo, sKullaniciAdi, dteKayitTarihi, sMagaza FROM tbOdeme WHERE nOdemeID > ?"
    )

    # Stok hareketleri
    total += sync_table(conn, 'tbStokFisiDetayi', 'nIslemID', max_ids.get('tbStokFisiDetayi', 0),
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
    total += sync_table(conn, 'tbMusteri', 'nMusteriID', max_ids.get('tbMusteri', 0),
        "SELECT nMusteriID, sAdi, sSoyadi, sGSM AS sTelefon1, sEvIl AS sIl "
        "FROM tbMusteri WHERE nMusteriID > ?"
    )

    conn.close()

    if total == 0:
        print("  Yeni kayit yok.")
    else:
        print(f"  Toplam {total} yeni kayit aktarildi.")

if __name__ == '__main__':
    print("=" * 50)
    print("Karpin -> Railway Otomatik Senkronizasyon")
    print(f"Her {SYNC_INTERVAL // 60} dakikada bir calisacak")
    print("Durdurmak icin Ctrl+C")
    print("=" * 50)

    while True:
        try:
            do_sync()
        except Exception as e:
            print(f"  HATA: {e}")
        time.sleep(SYNC_INTERVAL)
