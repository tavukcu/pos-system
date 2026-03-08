"""
SQL Server verilerini Railway'deki PostgreSQL'e HTTP API uzerinden aktar.
Kullanim: python upload_to_railway.py
"""
import pyodbc
import requests
import json
from datetime import datetime, date

API_URL = "https://pos-app-production-f14d.up.railway.app"
SECRET = "pos-migrate-2024"
BATCH_SIZE = 500

mssql = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=(local);DATABASE=BUSINESS2023;UID=sa;PWD=Ceddan1234;'
)

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

def upload_table(table, sql):
    print(f"\n{table} aktariliyor...")
    cursor = mssql.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
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
            print(f"  {uploaded}/{total} kayit aktarildi")
        except Exception as e:
            print(f"  HATA batch {i}: {e}")

    print(f"  {table}: toplam {uploaded}/{total} kayit aktarildi")


# 1) Tablolari olustur
print("Tablolar olusturuluyor...")
r = requests.post(f"{API_URL}/api/migrate/init", json={'secret': SECRET}, timeout=30)
print(f"  {r.json()}")

# 2) Verileri aktar
upload_table('tbStok',
    "SELECT nStokID, sKodu, sAciklama, sKisaAdi, nStokTipi, sBirimCinsi1, "
    "nIskontoYuzdesi, sKdvTipi, nTeminSuresi, lAsgariMiktar, lAzamiMiktar, "
    "sOzelNot, nFiyatlandirma, sModel, sKullaniciAdi, dteKayitTarihi, "
    "bEksiyeDusulebilirmi, sDefaultAsortiTipi, bEksideUyarsinmi, bOTVVar, "
    "sOTVTipi, nIskontoYuzdesiAV, bEk1, nEk2, nPrim, nEn, nBoy, nYukseklik, "
    "nHacim, nAgirlik, sDovizCinsi, sAlisKdvTipi, nButce, nKarlilik, sUlke "
    "FROM tbStok"
)

upload_table('tbStokBarkodu',
    "SELECT nStokID, sBarkod, nFirmaID, sKarsiStokKodu, sKarsiStokAciklama, "
    "sBirimCinsi, lBirimMiktar FROM tbStokBarkodu"
)

upload_table('tbStokFiyati',
    "SELECT nStokID, sFiyatTipi, lFiyat, dteFiyatTespitTarihi, sKullaniciAdi, "
    "dteKayitTarihi FROM tbStokFiyati"
)

upload_table('tbStokSinifi', "SELECT * FROM tbStokSinifi")

upload_table('tbMusteri',
    "SELECT nMusteriID, sAdi, sSoyadi, sGSM AS sTelefon1, sEvIl AS sIl FROM tbMusteri"
)

upload_table('tbAlisVeris',
    "SELECT nAlisverisID, sFisTipi, dteFaturaTarihi, nGirisCikis, lFaturaNo, "
    "nMusteriID, sMagaza, sKasiyerRumuzu, sAlisverisYapanAdi, sAlisverisYapanSoyadi, "
    "lToplamMiktar, lMalBedeli, lMalIskontoTutari, nDipIskontoYuzdesi, lDipIskontoTutari, "
    "nKdvOrani1, lKdvMatrahi1, lKdv1, nKdvOrani2, lKdvMatrahi2, lKdv2, "
    "nKdvOrani3, lKdvMatrahi3, lKdv3, nKdvOrani4, lKdvMatrahi4, lKdv4, "
    "nKdvOrani5, lKdvMatrahi5, lKdv5, lPesinat, nVadeFarkiYuzdesi, "
    "nVadeKdvOrani, lVadeKdvMatrahi, lVadeKdv, lVadeFarki, lNetTutar, "
    "sHareketTipi, bMuhasebeyeIslendimi, sKullaniciAdi, dteKayitTarihi "
    "FROM tbAlisVeris WHERE lNetTutar < 10000000"
)

upload_table('tbOdeme',
    "SELECT nOdemeID, nAlisverisID, sOdemeSekli, nOdemeKodu, sKasiyerRumuzu, "
    "dteOdemeTarihi, dteValorTarihi, lOdemeTutar, sDovizCinsi, lDovizTutar, "
    "lMakbuzNo, lOdemeNo, nTaksitID, nIadeAlisverisID, bMuhasebeyeIslendimi, "
    "nKasaNo, sKullaniciAdi, dteKayitTarihi, sMagaza FROM tbOdeme"
)

upload_table('tbStokFisiDetayi',
    "SELECT nIslemID, nStokID, dteIslemTarihi, nFirmaID, nMusteriID, sFisTipi, "
    "dteFisTarihi, lFisNo, nGirisCikis, sDepo, lReyonFisNo, sStokIslem, "
    "sKasiyerRumuzu, sSaticiRumuzu, sOdemeKodu, dteIrsaliyeTarihi, lIrsaliyeNo, "
    "lGirisMiktar1, lGirisMiktar2, lGirisFiyat, lGirisTutar, "
    "lCikisMiktar1, lCikisMiktar2, lCikisFiyat, lCikisTutar, "
    "sFiyatTipi, lBrutFiyat, lBrutTutar, lMaliyetFiyat, lMaliyetTutar, "
    "lIlaveMaliyetTutar, nIskontoYuzdesi, lIskontoTutari, sDovizCinsi, lDovizFiyat, "
    "nSiparisID, nReceteNo, nTransferID, sTransferDepo, nKdvOrani, nHesapID, "
    "sAciklama, sHareketTipi, bMuhasebeyeIslendimi, sKullaniciAdi, dteKayitTarihi, "
    "nAlisverisID, nStokFisiID, nIrsaliyeFisiID FROM tbStokFisiDetayi"
)

# 3) Indexleri olustur
print("\nIndexler olusturuluyor...")
r = requests.post(f"{API_URL}/api/migrate/index", json={'secret': SECRET}, timeout=30)
print(f"  {r.json()}")

print("\nAktarim tamamlandi!")
