"""
SQL Server -> PostgreSQL veri aktarma scripti.
Kullanim: python migrate_to_postgres.py <POSTGRESQL_URL>
Ornek:    python migrate_to_postgres.py "postgresql://user:pass@host:5432/railway"
"""
import sys
import pyodbc
import psycopg2

if len(sys.argv) < 2:
    print("Kullanim: python migrate_to_postgres.py <POSTGRESQL_URL>")
    sys.exit(1)

PG_URL = sys.argv[1]

# SQL Server baglantisi
mssql = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=(local);DATABASE=BUSINESS2023;UID=sa;PWD=Ceddan1234;'
)
mc = mssql.cursor()

# PostgreSQL baglantisi
pg = psycopg2.connect(PG_URL)
pc = pg.cursor()

print("PostgreSQL tablolari olusturuluyor...")

pc.execute("""
DROP TABLE IF EXISTS "tbStokFisiDetayi" CASCADE;
DROP TABLE IF EXISTS "tbStokFiyati" CASCADE;
DROP TABLE IF EXISTS "tbStokBarkodu" CASCADE;
DROP TABLE IF EXISTS "tbStokSinifi" CASCADE;
DROP TABLE IF EXISTS "tbOdeme" CASCADE;
DROP TABLE IF EXISTS "tbAlisVerisAdres" CASCADE;
DROP TABLE IF EXISTS "tbAlisVeris" CASCADE;
DROP TABLE IF EXISTS "tbMusteri" CASCADE;
DROP TABLE IF EXISTS "tbMusteriSinifi" CASCADE;
DROP TABLE IF EXISTS "tbMusteriNufusu" CASCADE;
DROP TABLE IF EXISTS "tbStok" CASCADE;

CREATE TABLE "tbStok" (
    "nStokID" INTEGER PRIMARY KEY,
    "sKodu" VARCHAR(20) NOT NULL DEFAULT '',
    "sAciklama" VARCHAR(60) NOT NULL DEFAULT '',
    "sKisaAdi" VARCHAR(20) NOT NULL DEFAULT '',
    "nStokTipi" NUMERIC NOT NULL DEFAULT 0,
    "sBirimCinsi1" VARCHAR(3) NOT NULL DEFAULT 'KG',
    "nIskontoYuzdesi" NUMERIC NOT NULL DEFAULT 0,
    "sKdvTipi" VARCHAR(10) NOT NULL DEFAULT '',
    "nTeminSuresi" NUMERIC NOT NULL DEFAULT 0,
    "lAsgariMiktar" NUMERIC NOT NULL DEFAULT 0,
    "lAzamiMiktar" NUMERIC NOT NULL DEFAULT 0,
    "sOzelNot" VARCHAR(255) NOT NULL DEFAULT '',
    "nFiyatlandirma" NUMERIC NOT NULL DEFAULT 0,
    "sModel" VARCHAR(20) NOT NULL DEFAULT '',
    "sKullaniciAdi" VARCHAR(60) NOT NULL DEFAULT '',
    "dteKayitTarihi" TIMESTAMP,
    "bEksiyeDusulebilirmi" BOOLEAN NOT NULL DEFAULT FALSE,
    "sDefaultAsortiTipi" VARCHAR(3) NOT NULL DEFAULT '',
    "bEksideUyarsinmi" BOOLEAN NOT NULL DEFAULT FALSE,
    "bOTVVar" BOOLEAN NOT NULL DEFAULT FALSE,
    "sOTVTipi" VARCHAR(10) NOT NULL DEFAULT '',
    "nIskontoYuzdesiAV" NUMERIC NOT NULL DEFAULT 0,
    "bEk1" BOOLEAN NOT NULL DEFAULT FALSE,
    "nEk2" SMALLINT NOT NULL DEFAULT 0,
    "nPrim" NUMERIC NOT NULL DEFAULT 0,
    "nEn" NUMERIC NOT NULL DEFAULT 0,
    "nBoy" NUMERIC NOT NULL DEFAULT 0,
    "nYukseklik" NUMERIC NOT NULL DEFAULT 0,
    "nHacim" NUMERIC NOT NULL DEFAULT 0,
    "nAgirlik" NUMERIC NOT NULL DEFAULT 0,
    "sDovizCinsi" VARCHAR(3) NOT NULL DEFAULT 'TL',
    "sAlisKdvTipi" VARCHAR(10) NOT NULL DEFAULT '',
    "nButce" NUMERIC NOT NULL DEFAULT 0,
    "nKarlilik" NUMERIC NOT NULL DEFAULT 0,
    "sUlke" VARCHAR(20) NOT NULL DEFAULT ''
);

CREATE TABLE "tbStokBarkodu" (
    "nStokID" INTEGER NOT NULL,
    "sBarkod" VARCHAR(20) NOT NULL DEFAULT '',
    "nFirmaID" INTEGER NOT NULL DEFAULT 0,
    "sKarsiStokKodu" VARCHAR(20) NOT NULL DEFAULT '',
    "sKarsiStokAciklama" VARCHAR(60) NOT NULL DEFAULT '',
    "sBirimCinsi" VARCHAR(3) NOT NULL DEFAULT '',
    "lBirimMiktar" NUMERIC NOT NULL DEFAULT 0
);

CREATE TABLE "tbStokFiyati" (
    "nStokID" INTEGER NOT NULL,
    "sFiyatTipi" VARCHAR(4) NOT NULL DEFAULT '',
    "lFiyat" NUMERIC(19,4) NOT NULL DEFAULT 0,
    "dteFiyatTespitTarihi" TIMESTAMP,
    "sKullaniciAdi" VARCHAR(60) NOT NULL DEFAULT '',
    "dteKayitTarihi" TIMESTAMP
);

CREATE TABLE "tbStokSinifi" (
    "nStokID" INTEGER PRIMARY KEY,
    "sSinifKodu1" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu2" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu3" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu4" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu5" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu6" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu7" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu8" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu9" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu10" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu11" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu12" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu13" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu14" VARCHAR(10) NOT NULL DEFAULT '',
    "sSinifKodu15" VARCHAR(10) NOT NULL DEFAULT ''
);

CREATE TABLE "tbMusteri" (
    "nMusteriID" INTEGER PRIMARY KEY,
    "sAdi" VARCHAR(60) NOT NULL DEFAULT '',
    "sSoyadi" VARCHAR(60) NOT NULL DEFAULT '',
    "sTelefon1" VARCHAR(30) DEFAULT '',
    "sIl" VARCHAR(30) DEFAULT ''
);

CREATE TABLE "tbAlisVeris" (
    "nAlisverisID" VARCHAR(20) NOT NULL PRIMARY KEY,
    "sFisTipi" VARCHAR(3) NOT NULL DEFAULT '',
    "dteFaturaTarihi" TIMESTAMP,
    "nGirisCikis" NUMERIC NOT NULL DEFAULT 0,
    "lFaturaNo" NUMERIC NOT NULL DEFAULT 0,
    "nMusteriID" INTEGER NOT NULL DEFAULT 0,
    "sMagaza" VARCHAR(4) NOT NULL DEFAULT '',
    "sKasiyerRumuzu" VARCHAR(4) NOT NULL DEFAULT '',
    "sAlisverisYapanAdi" VARCHAR(60) NOT NULL DEFAULT '',
    "sAlisverisYapanSoyadi" VARCHAR(60) NOT NULL DEFAULT '',
    "lToplamMiktar" NUMERIC NOT NULL DEFAULT 0,
    "lMalBedeli" NUMERIC NOT NULL DEFAULT 0,
    "lMalIskontoTutari" NUMERIC NOT NULL DEFAULT 0,
    "nDipIskontoYuzdesi" NUMERIC NOT NULL DEFAULT 0,
    "lDipIskontoTutari" NUMERIC NOT NULL DEFAULT 0,
    "nKdvOrani1" NUMERIC NOT NULL DEFAULT 0,
    "lKdvMatrahi1" NUMERIC NOT NULL DEFAULT 0,
    "lKdv1" NUMERIC NOT NULL DEFAULT 0,
    "nKdvOrani2" NUMERIC NOT NULL DEFAULT 0,
    "lKdvMatrahi2" NUMERIC NOT NULL DEFAULT 0,
    "lKdv2" NUMERIC NOT NULL DEFAULT 0,
    "nKdvOrani3" NUMERIC NOT NULL DEFAULT 0,
    "lKdvMatrahi3" NUMERIC NOT NULL DEFAULT 0,
    "lKdv3" NUMERIC NOT NULL DEFAULT 0,
    "nKdvOrani4" NUMERIC NOT NULL DEFAULT 0,
    "lKdvMatrahi4" NUMERIC NOT NULL DEFAULT 0,
    "lKdv4" NUMERIC NOT NULL DEFAULT 0,
    "nKdvOrani5" NUMERIC NOT NULL DEFAULT 0,
    "lKdvMatrahi5" NUMERIC NOT NULL DEFAULT 0,
    "lKdv5" NUMERIC NOT NULL DEFAULT 0,
    "lPesinat" NUMERIC NOT NULL DEFAULT 0,
    "nVadeFarkiYuzdesi" NUMERIC NOT NULL DEFAULT 0,
    "nVadeKdvOrani" NUMERIC NOT NULL DEFAULT 0,
    "lVadeKdvMatrahi" NUMERIC NOT NULL DEFAULT 0,
    "lVadeKdv" NUMERIC NOT NULL DEFAULT 0,
    "lVadeFarki" NUMERIC NOT NULL DEFAULT 0,
    "lNetTutar" NUMERIC NOT NULL DEFAULT 0,
    "sHareketTipi" VARCHAR(20) NOT NULL DEFAULT '',
    "bMuhasebeyeIslendimi" BOOLEAN NOT NULL DEFAULT FALSE,
    "sKullaniciAdi" VARCHAR(60) NOT NULL DEFAULT '',
    "dteKayitTarihi" TIMESTAMP
);

CREATE TABLE "tbOdeme" (
    "nOdemeID" VARCHAR(20) NOT NULL PRIMARY KEY,
    "nAlisverisID" VARCHAR(20) NOT NULL DEFAULT '',
    "sOdemeSekli" VARCHAR(4) NOT NULL DEFAULT '',
    "nOdemeKodu" NUMERIC NOT NULL DEFAULT 0,
    "sKasiyerRumuzu" VARCHAR(4) NOT NULL DEFAULT '',
    "dteOdemeTarihi" TIMESTAMP,
    "dteValorTarihi" TIMESTAMP,
    "lOdemeTutar" NUMERIC NOT NULL DEFAULT 0,
    "sDovizCinsi" VARCHAR(3) NOT NULL DEFAULT 'TL',
    "lDovizTutar" NUMERIC NOT NULL DEFAULT 0,
    "lMakbuzNo" NUMERIC NOT NULL DEFAULT 0,
    "lOdemeNo" NUMERIC NOT NULL DEFAULT 0,
    "nTaksitID" VARCHAR(20) NOT NULL DEFAULT '',
    "nIadeAlisverisID" VARCHAR(20) NOT NULL DEFAULT '',
    "bMuhasebeyeIslendimi" BOOLEAN NOT NULL DEFAULT FALSE,
    "nKasaNo" NUMERIC DEFAULT 0,
    "sKullaniciAdi" VARCHAR(60) NOT NULL DEFAULT '',
    "dteKayitTarihi" TIMESTAMP,
    "sMagaza" VARCHAR(4) NOT NULL DEFAULT ''
);

CREATE TABLE "tbStokFisiDetayi" (
    "nIslemID" NUMERIC NOT NULL,
    "nStokID" INTEGER NOT NULL DEFAULT 0,
    "dteIslemTarihi" TIMESTAMP,
    "nFirmaID" INTEGER NOT NULL DEFAULT 0,
    "nMusteriID" INTEGER NOT NULL DEFAULT 0,
    "sFisTipi" VARCHAR(3) NOT NULL DEFAULT '',
    "dteFisTarihi" TIMESTAMP,
    "lFisNo" NUMERIC NOT NULL DEFAULT 0,
    "nGirisCikis" NUMERIC NOT NULL DEFAULT 0,
    "sDepo" VARCHAR(4) NOT NULL DEFAULT '',
    "lReyonFisNo" NUMERIC NOT NULL DEFAULT 0,
    "sStokIslem" VARCHAR(3) NOT NULL DEFAULT '',
    "sKasiyerRumuzu" VARCHAR(4) NOT NULL DEFAULT '',
    "sSaticiRumuzu" VARCHAR(4) NOT NULL DEFAULT '',
    "sOdemeKodu" VARCHAR(4) NOT NULL DEFAULT '',
    "dteIrsaliyeTarihi" TIMESTAMP,
    "lIrsaliyeNo" NUMERIC NOT NULL DEFAULT 0,
    "lGirisMiktar1" NUMERIC NOT NULL DEFAULT 0,
    "lGirisMiktar2" NUMERIC NOT NULL DEFAULT 0,
    "lGirisFiyat" NUMERIC(19,4) NOT NULL DEFAULT 0,
    "lGirisTutar" NUMERIC NOT NULL DEFAULT 0,
    "lCikisMiktar1" NUMERIC NOT NULL DEFAULT 0,
    "lCikisMiktar2" NUMERIC NOT NULL DEFAULT 0,
    "lCikisFiyat" NUMERIC(19,4) NOT NULL DEFAULT 0,
    "lCikisTutar" NUMERIC NOT NULL DEFAULT 0,
    "sFiyatTipi" VARCHAR(4) NOT NULL DEFAULT '',
    "lBrutFiyat" NUMERIC(19,4) NOT NULL DEFAULT 0,
    "lBrutTutar" NUMERIC NOT NULL DEFAULT 0,
    "lMaliyetFiyat" NUMERIC(19,4) NOT NULL DEFAULT 0,
    "lMaliyetTutar" NUMERIC NOT NULL DEFAULT 0,
    "lIlaveMaliyetTutar" NUMERIC NOT NULL DEFAULT 0,
    "nIskontoYuzdesi" NUMERIC NOT NULL DEFAULT 0,
    "lIskontoTutari" NUMERIC NOT NULL DEFAULT 0,
    "sDovizCinsi" VARCHAR(3) NOT NULL DEFAULT 'TL',
    "lDovizFiyat" NUMERIC(19,4) NOT NULL DEFAULT 0,
    "nSiparisID" INTEGER NOT NULL DEFAULT 0,
    "nReceteNo" NUMERIC NOT NULL DEFAULT 0,
    "nTransferID" NUMERIC NOT NULL DEFAULT 0,
    "sTransferDepo" VARCHAR(4) NOT NULL DEFAULT '',
    "nKdvOrani" NUMERIC NOT NULL DEFAULT 0,
    "nHesapID" INTEGER NOT NULL DEFAULT 0,
    "sAciklama" VARCHAR(60) NOT NULL DEFAULT '',
    "sHareketTipi" VARCHAR(20) NOT NULL DEFAULT '',
    "bMuhasebeyeIslendimi" BOOLEAN NOT NULL DEFAULT FALSE,
    "sKullaniciAdi" VARCHAR(60) NOT NULL DEFAULT '',
    "dteKayitTarihi" TIMESTAMP,
    "nAlisverisID" VARCHAR(20) NOT NULL DEFAULT '',
    "nStokFisiID" NUMERIC NOT NULL DEFAULT 0,
    "nIrsaliyeFisiID" NUMERIC NOT NULL DEFAULT 0
);
""")
pg.commit()
print("Tablolar olusturuldu.")


def migrate_table(table, select_sql, insert_sql, transform=None):
    mc.execute(select_sql)
    cols = [d[0] for d in mc.description]
    rows = mc.fetchall()
    count = 0
    for row in rows:
        vals = list(row)
        if transform:
            vals = transform(vals, cols)
        # None ve string temizligi
        clean = []
        for v in vals:
            if isinstance(v, str):
                clean.append(v.strip() if v else '')
            elif v is None:
                clean.append(None)
            else:
                clean.append(v)
        try:
            pc.execute(insert_sql, clean)
            count += 1
        except Exception as e:
            pg.rollback()
            print(f"  HATA {table} satir {count}: {e}")
            continue
    pg.commit()
    print(f"  {table}: {count} kayit aktarildi")


# --- STOK ---
print("tbStok aktariliyor...")
migrate_table('tbStok',
    "SELECT nStokID, sKodu, sAciklama, sKisaAdi, nStokTipi, sBirimCinsi1, "
    "nIskontoYuzdesi, sKdvTipi, nTeminSuresi, lAsgariMiktar, lAzamiMiktar, "
    "sOzelNot, nFiyatlandirma, sModel, sKullaniciAdi, dteKayitTarihi, "
    "bEksiyeDusulebilirmi, sDefaultAsortiTipi, bEksideUyarsinmi, bOTVVar, "
    "sOTVTipi, nIskontoYuzdesiAV, bEk1, nEk2, nPrim, nEn, nBoy, nYukseklik, "
    "nHacim, nAgirlik, sDovizCinsi, sAlisKdvTipi, nButce, nKarlilik, sUlke "
    "FROM tbStok",
    'INSERT INTO "tbStok" VALUES (' + ','.join(['%s']*35) + ')'
)

# --- BARKOD ---
print("tbStokBarkodu aktariliyor...")
migrate_table('tbStokBarkodu',
    "SELECT nStokID, sBarkod, nFirmaID, sKarsiStokKodu, sKarsiStokAciklama, sBirimCinsi, lBirimMiktar FROM tbStokBarkodu",
    'INSERT INTO "tbStokBarkodu" VALUES (' + ','.join(['%s']*7) + ')'
)

# --- FIYAT ---
print("tbStokFiyati aktariliyor...")
migrate_table('tbStokFiyati',
    "SELECT nStokID, sFiyatTipi, lFiyat, dteFiyatTespitTarihi, sKullaniciAdi, dteKayitTarihi FROM tbStokFiyati",
    'INSERT INTO "tbStokFiyati" VALUES (' + ','.join(['%s']*6) + ')'
)

# --- STOK SINIFI ---
print("tbStokSinifi aktariliyor...")
migrate_table('tbStokSinifi',
    "SELECT * FROM tbStokSinifi",
    'INSERT INTO "tbStokSinifi" VALUES (' + ','.join(['%s']*16) + ')'
)

# --- MUSTERI ---
print("tbMusteri aktariliyor...")
migrate_table('tbMusteri',
    "SELECT nMusteriID, sAdi, sSoyadi, sTelefon1, sIl FROM tbMusteri",
    'INSERT INTO "tbMusteri" VALUES (' + ','.join(['%s']*5) + ')'
)

# --- ALISVERIS (anormal kayitlar haric) ---
print("tbAlisVeris aktariliyor...")
migrate_table('tbAlisVeris',
    "SELECT nAlisverisID, sFisTipi, dteFaturaTarihi, nGirisCikis, lFaturaNo, "
    "nMusteriID, sMagaza, sKasiyerRumuzu, sAlisverisYapanAdi, sAlisverisYapanSoyadi, "
    "lToplamMiktar, lMalBedeli, lMalIskontoTutari, nDipIskontoYuzdesi, lDipIskontoTutari, "
    "nKdvOrani1, lKdvMatrahi1, lKdv1, nKdvOrani2, lKdvMatrahi2, lKdv2, "
    "nKdvOrani3, lKdvMatrahi3, lKdv3, nKdvOrani4, lKdvMatrahi4, lKdv4, "
    "nKdvOrani5, lKdvMatrahi5, lKdv5, lPesinat, nVadeFarkiYuzdesi, "
    "nVadeKdvOrani, lVadeKdvMatrahi, lVadeKdv, lVadeFarki, lNetTutar, "
    "sHareketTipi, bMuhasebeyeIslendimi, sKullaniciAdi, dteKayitTarihi "
    "FROM tbAlisVeris WHERE lNetTutar < 10000000",
    'INSERT INTO "tbAlisVeris" VALUES (' + ','.join(['%s']*41) + ')'
)

# --- ODEME ---
print("tbOdeme aktariliyor...")
migrate_table('tbOdeme',
    "SELECT nOdemeID, nAlisverisID, sOdemeSekli, nOdemeKodu, sKasiyerRumuzu, "
    "dteOdemeTarihi, dteValorTarihi, lOdemeTutar, sDovizCinsi, lDovizTutar, "
    "lMakbuzNo, lOdemeNo, nTaksitID, nIadeAlisverisID, bMuhasebeyeIslendimi, "
    "nKasaNo, sKullaniciAdi, dteKayitTarihi, sMagaza FROM tbOdeme",
    'INSERT INTO "tbOdeme" VALUES (' + ','.join(['%s']*19) + ')'
)

# --- STOK FISI DETAYI ---
print("tbStokFisiDetayi aktariliyor (bu uzun surebilir)...")
migrate_table('tbStokFisiDetayi',
    "SELECT nIslemID, nStokID, dteIslemTarihi, nFirmaID, nMusteriID, sFisTipi, "
    "dteFisTarihi, lFisNo, nGirisCikis, sDepo, lReyonFisNo, sStokIslem, "
    "sKasiyerRumuzu, sSaticiRumuzu, sOdemeKodu, dteIrsaliyeTarihi, lIrsaliyeNo, "
    "lGirisMiktar1, lGirisMiktar2, lGirisFiyat, lGirisTutar, "
    "lCikisMiktar1, lCikisMiktar2, lCikisFiyat, lCikisTutar, "
    "sFiyatTipi, lBrutFiyat, lBrutTutar, lMaliyetFiyat, lMaliyetTutar, "
    "lIlaveMaliyetTutar, nIskontoYuzdesi, lIskontoTutari, sDovizCinsi, lDovizFiyat, "
    "nSiparisID, nReceteNo, nTransferID, sTransferDepo, nKdvOrani, nHesapID, "
    "sAciklama, sHareketTipi, bMuhasebeyeIslendimi, sKullaniciAdi, dteKayitTarihi, "
    "nAlisverisID, nStokFisiID, nIrsaliyeFisiID FROM tbStokFisiDetayi",
    'INSERT INTO "tbStokFisiDetayi" VALUES (' + ','.join(['%s']*49) + ')'
)

# --- INDEXLER ---
print("Indexler olusturuluyor...")
pc.execute("""
CREATE INDEX idx_stok_kod ON "tbStok" ("sKodu");
CREATE INDEX idx_stok_aciklama ON "tbStok" ("sAciklama");
CREATE INDEX idx_barkod_barkod ON "tbStokBarkodu" ("sBarkod");
CREATE INDEX idx_barkod_stokid ON "tbStokBarkodu" ("nStokID");
CREATE INDEX idx_fiyat_stokid ON "tbStokFiyati" ("nStokID", "sFiyatTipi");
CREATE INDEX idx_av_tarih ON "tbAlisVeris" ("dteFaturaTarihi");
CREATE INDEX idx_av_fistipi ON "tbAlisVeris" ("sFisTipi");
CREATE INDEX idx_sfd_tarih ON "tbStokFisiDetayi" ("dteIslemTarihi");
CREATE INDEX idx_sfd_stokid ON "tbStokFisiDetayi" ("nStokID");
CREATE INDEX idx_sfd_avid ON "tbStokFisiDetayi" ("nAlisverisID");
CREATE INDEX idx_odeme_avid ON "tbOdeme" ("nAlisverisID");
""")
pg.commit()
print("Indexler olusturuldu.")

mssql.close()
pg.close()
print("\nAktarim tamamlandi!")
