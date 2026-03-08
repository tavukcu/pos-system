from flask import Flask, render_template, request, jsonify
from config import query, execute, get_connection
from datetime import datetime, date
import traceback

app = Flask(__name__)
app.json.ensure_ascii = False

@app.errorhandler(Exception)
def handle_error(e):
    return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

# --- HELPERS ---

def get_next_stok_id():
    result = query("SELECT MIN(nStokID) AS minid FROM tbStok")
    return int(result[0]['minid']) - 1

def get_next_alisveris_id():
    result = query("SELECT TOP 1 nAlisverisID FROM tbAlisVeris ORDER BY nAlisverisID DESC")
    if result:
        last_id = result[0]['nAlisverisID'].strip()
        prefix = ''.join(c for c in last_id if c.isalpha())
        num = int(''.join(c for c in last_id if c.isdigit()))
        return f"{prefix}{num + 1:08d}"
    return "D00000001"

def get_next_fis_no(fis_tipi):
    today = date.today()
    result = query(
        "SELECT ISNULL(MAX(lFaturaNo), 0) AS maxno FROM tbAlisVeris "
        "WHERE sFisTipi = ? AND CAST(dteFaturaTarihi AS DATE) = ?",
        [fis_tipi, today]
    )
    return int(result[0]['maxno']) + 1

def get_next_islem_id():
    result = query("SELECT ISNULL(MAX(nIslemID), 0) AS maxid FROM tbStokFisiDetayi")
    return int(result[0]['maxid']) + 1

def get_next_odeme_id():
    result = query("SELECT TOP 1 nOdemeID FROM tbOdeme ORDER BY nOdemeID DESC")
    if result:
        last_id = result[0]['nOdemeID'].strip()
        prefix = ''.join(c for c in last_id if c.isalpha())
        num = int(''.join(c for c in last_id if c.isdigit()))
        return f"{prefix}{num + 1:08d}"
    return "O00000001"

def parse_tabak_barkod(barkod):
    """Pilic sektoru tabak barkod formati:
    28PPPPPWWWWWC  (13 hane)
    - 28: tartili urun prefix
    - PPPPP: urun kodu (5 hane)
    - WWWWW: agirlik gram (5 hane)
    - C: check digit
    Veya kisa barkod (5-7 hane) dogrudan urun kodu olabilir.
    """
    info = {'barkod': barkod, 'urun_kodu': '', 'agirlik': 0, 'tartili': False}

    if len(barkod) == 13 and barkod.startswith('28'):
        info['urun_kodu'] = barkod[2:7]
        info['agirlik'] = int(barkod[7:12]) / 1000.0  # gram -> kg
        info['tartili'] = True
    elif len(barkod) == 13 and barkod.startswith('29'):
        info['urun_kodu'] = barkod[2:7]
        info['agirlik'] = int(barkod[7:12]) / 1000.0
        info['tartili'] = True
    elif len(barkod) == 13 and barkod.startswith('2'):
        info['urun_kodu'] = barkod[1:6]
        info['agirlik'] = int(barkod[7:12]) / 1000.0
        info['tartili'] = True
    else:
        info['urun_kodu'] = barkod

    return info


# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/pos')
def pos():
    return render_template('pos.html')

@app.route('/urunler')
def urunler():
    return render_template('urunler.html')

@app.route('/musteriler')
def musteriler():
    return render_template('musteriler.html')

@app.route('/raporlar')
def raporlar():
    return render_template('raporlar.html')


# --- API: URUNLER ---

FIYAT_JOIN = (
    "LEFT JOIN (SELECT nStokID, lFiyat FROM tbStokFiyati "
    "WHERE sFiyatTipi = '1') f ON s.nStokID = f.nStokID "
)

URUN_SELECT = (
    "s.nStokID, s.sKodu, s.sAciklama, "
    "ISNULL(f.lFiyat, 0) AS fiyat, s.sBirimCinsi1 "
)

def format_urun(r):
    return {
        'id': r['nStokID'],
        'kod': (r['sKodu'] or '').strip(),
        'ad': (r['sAciklama'] or '').strip(),
        'fiyat': float(r['fiyat'] or 0),
        'birim': (r['sBirimCinsi1'] or 'AD').strip(),
    }

@app.route('/api/urunler')
def api_urunler():
    search = request.args.get('q', '')
    if search:
        rows = query(
            f"SELECT TOP 50 {URUN_SELECT} FROM tbStok s {FIYAT_JOIN}"
            "WHERE s.sKodu LIKE ? OR s.sAciklama LIKE ? "
            "ORDER BY s.sAciklama",
            [f'%{search}%', f'%{search}%']
        )
    else:
        rows = query(
            f"SELECT TOP 50 {URUN_SELECT} FROM tbStok s {FIYAT_JOIN}"
            "ORDER BY s.sAciklama"
        )
    return jsonify([format_urun(r) for r in rows])


@app.route('/api/barkod/<path:barkod>')
def api_barkod(barkod):
    barkod = barkod.strip()

    # 1) Tam barkod eslesme
    rows = query(
        f"SELECT {URUN_SELECT} FROM tbStokBarkodu b "
        f"JOIN tbStok s ON b.nStokID = s.nStokID {FIYAT_JOIN}"
        "WHERE RTRIM(b.sBarkod) = ?",
        [barkod]
    )
    if rows:
        r = format_urun(rows[0])
        parsed = parse_tabak_barkod(barkod)
        r.update({'found': True, 'agirlik': parsed['agirlik'] if parsed['tartili'] else 0, 'tartili': parsed['tartili']})
        return jsonify(r)

    # 2) Tartili barkodsa (28xxxxx) urun kodunu ayikla ve ona gore ara
    parsed = parse_tabak_barkod(barkod)
    if parsed['tartili']:
        rows = query(
            f"SELECT {URUN_SELECT} FROM tbStokBarkodu b "
            f"JOIN tbStok s ON b.nStokID = s.nStokID {FIYAT_JOIN}"
            "WHERE RTRIM(b.sBarkod) = ?",
            [parsed['urun_kodu']]
        )
        if rows:
            r = format_urun(rows[0])
            r.update({'found': True, 'agirlik': parsed['agirlik'], 'tartili': True})
            return jsonify(r)

    # 3) Stok kodu ile eslesme dene
    rows = query(
        f"SELECT {URUN_SELECT} FROM tbStok s {FIYAT_JOIN}"
        "WHERE RTRIM(s.sKodu) = ?",
        [barkod]
    )
    if rows:
        r = format_urun(rows[0])
        r.update({'found': True, 'agirlik': parsed['agirlik'] if parsed['tartili'] else 0, 'tartili': parsed['tartili']})
        return jsonify(r)

    # 4) Bulunamadi - barkod bilgilerini don
    return jsonify({
        'found': False,
        'barkod': barkod,
        'urun_kodu': parsed['urun_kodu'],
        'agirlik': parsed['agirlik'],
        'tartili': parsed['tartili'],
    })


# --- API: URUN EKLE ---

@app.route('/api/urun_ekle', methods=['POST'])
def api_urun_ekle():
    data = request.json
    barkod = data.get('barkod', '').strip()
    urun_kodu = data.get('urun_kodu', '').strip()
    urun_adi = data.get('urun_adi', '').strip()
    kisa_adi = data.get('kisa_adi', '').strip()
    birim = data.get('birim', 'KG').strip()
    fiyat = float(data.get('fiyat', 0))
    kdv_tipi = data.get('kdv_tipi', '02').strip()

    if not urun_adi or not urun_kodu:
        return jsonify({'error': 'Urun adi ve kodu zorunludur'}), 400

    # Stok kodu benzersiz mi kontrol et
    existing = query("SELECT nStokID FROM tbStok WHERE RTRIM(sKodu) = ?", [urun_kodu])
    if existing:
        return jsonify({'error': f'Bu stok kodu zaten mevcut: {urun_kodu}'}), 400

    if not kisa_adi:
        kisa_adi = urun_adi[:20]

    now = datetime.now()
    stok_id = get_next_stok_id()

    conn = get_connection()
    cursor = conn.cursor()
    try:
        # tbStok kaydı
        cursor.execute(
            "INSERT INTO tbStok (nStokID, sKodu, sAciklama, sKisaAdi, nStokTipi, "
            "sBirimCinsi1, nIskontoYuzdesi, sKdvTipi, nTeminSuresi, "
            "lAsgariMiktar, lAzamiMiktar, sOzelNot, nFiyatlandirma, sModel, "
            "sKullaniciAdi, dteKayitTarihi, bEksiyeDusulebilirmi, sDefaultAsortiTipi, "
            "bEksideUyarsinmi, bOTVVar, sOTVTipi, nIskontoYuzdesiAV, bEk1, nEk2, "
            "nPrim, nEn, nBoy, nYukseklik, nHacim, nAgirlik, sDovizCinsi, "
            "sAlisKdvTipi, nButce, nKarlilik, sUlke) "
            "VALUES (?, ?, ?, ?, 0, ?, 0, ?, 0, 0, 0, '', 0, '', "
            "'POS', ?, 0, '', 0, 0, '', 0, 0, 0, 0, 0, 0, 0, 0, 0, 'TL', ?, 0, 0, '')",
            [stok_id, urun_kodu, urun_adi, kisa_adi, birim, kdv_tipi, now, kdv_tipi]
        )

        # tbStokBarkodu kaydı
        cursor.execute(
            "INSERT INTO tbStokBarkodu (nStokID, sBarkod, nFirmaID, sKarsiStokKodu, "
            "sKarsiStokAciklama, sBirimCinsi, lBirimMiktar) "
            "VALUES (?, ?, 0, '', '', ?, 0)",
            [stok_id, barkod, birim]
        )

        # Tartili urun ise kisaltilmis barkodu da ekle (urun kodu kismi)
        parsed = parse_tabak_barkod(barkod)
        if parsed['tartili'] and parsed['urun_kodu'] != barkod:
            cursor.execute(
                "INSERT INTO tbStokBarkodu (nStokID, sBarkod, nFirmaID, sKarsiStokKodu, "
                "sKarsiStokAciklama, sBirimCinsi, lBirimMiktar) "
                "VALUES (?, ?, 0, '', '', ?, 0)",
                [stok_id, parsed['urun_kodu'], birim]
            )

        # tbStokFiyati kaydı (satis fiyati)
        if fiyat > 0:
            cursor.execute(
                "INSERT INTO tbStokFiyati (nStokID, sFiyatTipi, lFiyat, "
                "dteFiyatTespitTarihi, sKullaniciAdi, dteKayitTarihi) "
                "VALUES (?, '1', ?, ?, 'POS', ?)",
                [stok_id, fiyat, now, now]
            )

        # tbStokSinifi kaydı (bos sinif)
        cursor.execute(
            "INSERT INTO tbStokSinifi (nStokID, sSinifKodu1, sSinifKodu2, sSinifKodu3, "
            "sSinifKodu4, sSinifKodu5, sSinifKodu6, sSinifKodu7, sSinifKodu8, "
            "sSinifKodu9, sSinifKodu10, sSinifKodu11, sSinifKodu12, sSinifKodu13, "
            "sSinifKodu14, sSinifKodu15) "
            "VALUES (?, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '')",
            [stok_id]
        )

        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'error': str(e)}), 500

    conn.close()
    return jsonify({
        'ok': True,
        'id': stok_id,
        'kod': urun_kodu,
        'ad': kisa_adi,
        'fiyat': fiyat,
        'birim': birim,
        'agirlik': parsed['agirlik'] if parsed['tartili'] else 0,
        'tartili': parsed['tartili'],
    })


# --- API: SATIS ---

@app.route('/api/satis', methods=['POST'])
def api_satis():
    data = request.json
    kalemler = data.get('kalemler', [])
    odeme_sekli = data.get('odeme_sekli', 'N')
    musteri_id = data.get('musteri_id', 0)
    musteri_adi = data.get('musteri_adi', '')
    musteri_soyadi = data.get('musteri_soyadi', '')

    if not kalemler:
        return jsonify({'error': 'Sepet bos'}), 400

    toplam_miktar = sum(float(k['miktar']) for k in kalemler)
    toplam_tutar = sum(float(k['miktar']) * float(k['fiyat']) for k in kalemler)

    fis_tipi = 'P'
    alisveris_id = get_next_alisveris_id()
    fis_no = get_next_fis_no(fis_tipi)
    now = datetime.now()

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO tbAlisVeris (nAlisverisID, sFisTipi, dteFaturaTarihi, "
            "nGirisCikis, lFaturaNo, nMusteriID, sMagaza, sKasiyerRumuzu, "
            "sAlisverisYapanAdi, sAlisverisYapanSoyadi, lToplamMiktar, lMalBedeli, "
            "lMalIskontoTutari, nDipIskontoYuzdesi, lDipIskontoTutari, "
            "nKdvOrani1, lKdvMatrahi1, lKdv1, nKdvOrani2, lKdvMatrahi2, lKdv2, "
            "nKdvOrani3, lKdvMatrahi3, lKdv3, nKdvOrani4, lKdvMatrahi4, lKdv4, "
            "nKdvOrani5, lKdvMatrahi5, lKdv5, lPesinat, nVadeFarkiYuzdesi, "
            "nVadeKdvOrani, lVadeKdvMatrahi, lVadeKdv, lVadeFarki, "
            "lNetTutar, sHareketTipi, bMuhasebeyeIslendimi, "
            "sKullaniciAdi, dteKayitTarihi) "
            "VALUES (?, ?, ?, 3, ?, ?, 'D001', '', ?, ?, ?, ?, "
            "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "
            "0, 0, 0, 0, 0, 0, ?, '', 0, 'POS', ?)",
            [alisveris_id, fis_tipi, now, fis_no, musteri_id,
             musteri_adi, musteri_soyadi, toplam_miktar, toplam_tutar,
             toplam_tutar, now]
        )

        islem_id = get_next_islem_id()
        for k in kalemler:
            miktar = float(k['miktar'])
            fiyat = float(k['fiyat'])
            tutar = miktar * fiyat
            cursor.execute(
                "INSERT INTO tbStokFisiDetayi (nIslemID, nStokID, dteIslemTarihi, "
                "nFirmaID, nMusteriID, sFisTipi, dteFisTarihi, lFisNo, nGirisCikis, "
                "sDepo, lReyonFisNo, sStokIslem, sKasiyerRumuzu, sSaticiRumuzu, "
                "sOdemeKodu, dteIrsaliyeTarihi, lIrsaliyeNo, "
                "lGirisMiktar1, lGirisMiktar2, lGirisFiyat, lGirisTutar, "
                "lCikisMiktar1, lCikisMiktar2, lCikisFiyat, lCikisTutar, "
                "sFiyatTipi, lBrutFiyat, lBrutTutar, lMaliyetFiyat, lMaliyetTutar, "
                "lIlaveMaliyetTutar, nIskontoYuzdesi, lIskontoTutari, "
                "sDovizCinsi, lDovizFiyat, nSiparisID, nReceteNo, nTransferID, "
                "sTransferDepo, nKdvOrani, nHesapID, sAciklama, sHareketTipi, "
                "bMuhasebeyeIslendimi, sKullaniciAdi, dteKayitTarihi, "
                "nAlisverisID, nStokFisiID, nIrsaliyeFisiID) "
                "VALUES (?, ?, ?, 0, ?, ?, ?, ?, 3, 'D001', 0, '', '', '', "
                "?, ?, 0, 0, 0, 0, 0, ?, 0, ?, ?, '', 0, 0, 0, 0, 0, 0, 0, "
                "'TL', 0, 0, 0, 0, '', 0, 0, '', '', 0, 'POS', ?, ?, 0, 0)",
                [islem_id, k['stok_id'], now, musteri_id, fis_tipi, now,
                 fis_no, odeme_sekli, now, miktar, fiyat, tutar, now, alisveris_id]
            )
            islem_id += 1

        odeme_id = get_next_odeme_id()
        cursor.execute(
            "INSERT INTO tbOdeme (nOdemeID, nAlisverisID, sOdemeSekli, "
            "nOdemeKodu, sKasiyerRumuzu, dteOdemeTarihi, dteValorTarihi, "
            "lOdemeTutar, sDovizCinsi, lDovizTutar, lMakbuzNo, lOdemeNo, "
            "nTaksitID, nIadeAlisverisID, bMuhasebeyeIslendimi, nKasaNo, "
            "sKullaniciAdi, dteKayitTarihi, sMagaza) "
            "VALUES (?, ?, ?, 0, '', ?, ?, ?, 'TL', 0, 0, 0, '', '', 0, 1, "
            "'POS', ?, 'D001')",
            [odeme_id, alisveris_id, odeme_sekli, now, now, toplam_tutar, now]
        )

        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'error': str(e)}), 500

    conn.close()
    return jsonify({
        'ok': True,
        'alisveris_id': alisveris_id.strip(),
        'fis_no': fis_no,
        'toplam': toplam_tutar
    })


# --- API: MUSTERILER ---

@app.route('/api/musteriler')
def api_musteriler():
    search = request.args.get('q', '')
    if search:
        rows = query(
            "SELECT TOP 50 nMusteriID, sAdi, sSoyadi, sTelefon1, sIl "
            "FROM tbMusteri WHERE sAdi LIKE ? OR sSoyadi LIKE ? OR sTelefon1 LIKE ? "
            "ORDER BY sAdi",
            [f'%{search}%', f'%{search}%', f'%{search}%']
        )
    else:
        rows = query(
            "SELECT TOP 50 nMusteriID, sAdi, sSoyadi, sTelefon1, sIl "
            "FROM tbMusteri ORDER BY sAdi"
        )
    return jsonify([{
        'id': r['nMusteriID'],
        'adi': (r['sAdi'] or '').strip(),
        'soyadi': (r['sSoyadi'] or '').strip(),
        'telefon': (r['sTelefon1'] or '').strip(),
        'il': (r['sIl'] or '').strip(),
    } for r in rows])


# --- API: RAPORLAR ---

@app.route('/api/rapor/gunluk')
def api_rapor_gunluk():
    tarih = request.args.get('tarih', date.today().isoformat())
    rows = query(
        "SELECT COUNT(*) AS islem_adedi, "
        "ISNULL(SUM(lNetTutar), 0) AS toplam_ciro, "
        "ISNULL(AVG(lNetTutar), 0) AS ort_fis "
        "FROM tbAlisVeris WHERE CAST(dteFaturaTarihi AS DATE) = ? "
        "AND lNetTutar < 10000000",
        [tarih]
    )
    return jsonify({
        'tarih': tarih,
        'islem_adedi': int(rows[0]['islem_adedi']),
        'toplam_ciro': float(rows[0]['toplam_ciro']),
        'ort_fis': float(rows[0]['ort_fis']),
    })

@app.route('/api/rapor/aylik')
def api_rapor_aylik():
    yil = request.args.get('yil', date.today().year)
    rows = query(
        "SELECT MONTH(dteFaturaTarihi) AS ay, COUNT(*) AS islem_adedi, "
        "SUM(lNetTutar) AS toplam_ciro, AVG(lNetTutar) AS ort_fis "
        "FROM tbAlisVeris WHERE YEAR(dteFaturaTarihi) = ? "
        "AND lNetTutar < 10000000 "
        "GROUP BY MONTH(dteFaturaTarihi) ORDER BY ay",
        [int(yil)]
    )
    return jsonify([{
        'ay': int(r['ay']),
        'islem_adedi': int(r['islem_adedi']),
        'toplam_ciro': float(r['toplam_ciro']),
        'ort_fis': float(r['ort_fis']),
    } for r in rows])

@app.route('/api/rapor/en_cok_satan')
def api_rapor_en_cok_satan():
    gun = request.args.get('gun', 30)
    rows = query(
        "SELECT TOP 20 s.sAciklama, SUM(d.lCikisMiktar1) AS toplam_miktar, "
        "SUM(d.lCikisTutar) AS toplam_tutar "
        "FROM tbStokFisiDetayi d "
        "JOIN tbStok s ON d.nStokID = s.nStokID "
        "WHERE d.dteIslemTarihi >= DATEADD(DAY, ?, GETDATE()) "
        "AND d.lCikisTutar < 10000000 AND d.nGirisCikis = 3 "
        "GROUP BY s.sAciklama ORDER BY toplam_tutar DESC",
        [-int(gun)]
    )
    return jsonify([{
        'urun': (r['sAciklama'] or '').strip(),
        'miktar': float(r['toplam_miktar']),
        'tutar': float(r['toplam_tutar']),
    } for r in rows])

@app.route('/api/rapor/son_satislar')
def api_rapor_son_satislar():
    rows = query(
        "SELECT TOP 50 a.nAlisverisID, a.sFisTipi, a.dteFaturaTarihi, "
        "a.lFaturaNo, a.sAlisverisYapanAdi, a.sAlisverisYapanSoyadi, "
        "a.lToplamMiktar, a.lNetTutar, a.sKullaniciAdi "
        "FROM tbAlisVeris a WHERE a.lNetTutar < 10000000 "
        "ORDER BY a.dteFaturaTarihi DESC, a.lFaturaNo DESC"
    )
    return jsonify([{
        'id': r['nAlisverisID'].strip(),
        'fis_tipi': (r['sFisTipi'] or '').strip(),
        'tarih': r['dteFaturaTarihi'].strftime('%d.%m.%Y') if r['dteFaturaTarihi'] else '',
        'fis_no': int(r['lFaturaNo']),
        'musteri': f"{(r['sAlisverisYapanAdi'] or '').strip()} {(r['sAlisverisYapanSoyadi'] or '').strip()}".strip(),
        'miktar': float(r['lToplamMiktar']),
        'tutar': float(r['lNetTutar']),
    } for r in rows])


# --- API: MIGRATION (uzaktan veri aktarimi) ---

MIGRATE_TABLES_SQL = """
DROP TABLE IF EXISTS tbstokfisidetayi CASCADE;
DROP TABLE IF EXISTS tbstokfiyati CASCADE;
DROP TABLE IF EXISTS tbstokbarkodu CASCADE;
DROP TABLE IF EXISTS tbstoksinifi CASCADE;
DROP TABLE IF EXISTS tbodeme CASCADE;
DROP TABLE IF EXISTS tbalisveris CASCADE;
DROP TABLE IF EXISTS tbmusteri CASCADE;
DROP TABLE IF EXISTS tbstok CASCADE;

CREATE TABLE tbstok (
    nstokid INTEGER PRIMARY KEY, skodu VARCHAR(20) DEFAULT '', saciklama VARCHAR(60) DEFAULT '',
    skisaadi VARCHAR(20) DEFAULT '', nstoktipi NUMERIC DEFAULT 0, sbirimcinsi1 VARCHAR(3) DEFAULT 'KG',
    niskontoyuzdesi NUMERIC DEFAULT 0, skdvtipi VARCHAR(10) DEFAULT '', nteminsuresi NUMERIC DEFAULT 0,
    lasgarimiktar NUMERIC DEFAULT 0, lazamimiktar NUMERIC DEFAULT 0, sozelnot VARCHAR(255) DEFAULT '',
    nfiyatlandirma NUMERIC DEFAULT 0, smodel VARCHAR(20) DEFAULT '', skullaniciadi VARCHAR(60) DEFAULT '',
    dtekayittarihi TIMESTAMP, beksiyedusulebilirmi BOOLEAN DEFAULT FALSE, sdefaultasortitipi VARCHAR(3) DEFAULT '',
    beksideuyarsinmi BOOLEAN DEFAULT FALSE, botvvar BOOLEAN DEFAULT FALSE, sotvtipi VARCHAR(10) DEFAULT '',
    niskontoyuzdesiav NUMERIC DEFAULT 0, bek1 BOOLEAN DEFAULT FALSE, nek2 SMALLINT DEFAULT 0,
    nprim NUMERIC DEFAULT 0, nen NUMERIC DEFAULT 0, nboy NUMERIC DEFAULT 0, nyukseklik NUMERIC DEFAULT 0,
    nhacim NUMERIC DEFAULT 0, nagirlik NUMERIC DEFAULT 0, sdovizcinsi VARCHAR(3) DEFAULT 'TL',
    saliskdvtipi VARCHAR(10) DEFAULT '', nbutce NUMERIC DEFAULT 0, nkarlilik NUMERIC DEFAULT 0, sulke VARCHAR(20) DEFAULT ''
);
CREATE TABLE tbstokbarkodu (
    nstokid INTEGER NOT NULL, sbarkod VARCHAR(20) DEFAULT '', nfirmaid INTEGER DEFAULT 0,
    skarsistokkodu VARCHAR(20) DEFAULT '', skarsistokaciklama VARCHAR(60) DEFAULT '',
    sbirimcinsi VARCHAR(3) DEFAULT '', lbirimmiktar NUMERIC DEFAULT 0
);
CREATE TABLE tbstokfiyati (
    nstokid INTEGER NOT NULL, sfiyattipi VARCHAR(4) DEFAULT '', lfiyat NUMERIC(19,4) DEFAULT 0,
    dtefiyattespittarihi TIMESTAMP, skullaniciadi VARCHAR(60) DEFAULT '', dtekayittarihi TIMESTAMP
);
CREATE TABLE tbstoksinifi (
    nstokid INTEGER PRIMARY KEY, ssinifkodu1 VARCHAR(10) DEFAULT '', ssinifkodu2 VARCHAR(10) DEFAULT '',
    ssinifkodu3 VARCHAR(10) DEFAULT '', ssinifkodu4 VARCHAR(10) DEFAULT '', ssinifkodu5 VARCHAR(10) DEFAULT '',
    ssinifkodu6 VARCHAR(10) DEFAULT '', ssinifkodu7 VARCHAR(10) DEFAULT '', ssinifkodu8 VARCHAR(10) DEFAULT '',
    ssinifkodu9 VARCHAR(10) DEFAULT '', ssinifkodu10 VARCHAR(10) DEFAULT '', ssinifkodu11 VARCHAR(10) DEFAULT '',
    ssinifkodu12 VARCHAR(10) DEFAULT '', ssinifkodu13 VARCHAR(10) DEFAULT '', ssinifkodu14 VARCHAR(10) DEFAULT '',
    ssinifkodu15 VARCHAR(10) DEFAULT ''
);
CREATE TABLE tbmusteri (
    nmusteriid INTEGER PRIMARY KEY, sadi VARCHAR(60) DEFAULT '', ssoyadi VARCHAR(60) DEFAULT '',
    stelefon1 VARCHAR(30) DEFAULT '', sil VARCHAR(30) DEFAULT ''
);
CREATE TABLE tbalisveris (
    nalisverisid VARCHAR(20) PRIMARY KEY, sfistipi VARCHAR(3) DEFAULT '', dtefaturatarihi TIMESTAMP,
    ngiriscikis NUMERIC DEFAULT 0, lfaturano NUMERIC DEFAULT 0, nmusteriid INTEGER DEFAULT 0,
    smagaza VARCHAR(4) DEFAULT '', skasiyerrumuzu VARCHAR(4) DEFAULT '',
    salisverisyapanadi VARCHAR(60) DEFAULT '', salisverisyapansoyadi VARCHAR(60) DEFAULT '',
    ltoplammiktar NUMERIC DEFAULT 0, lmalbedeli NUMERIC DEFAULT 0, lmaliskontotutari NUMERIC DEFAULT 0,
    ndipiskontoyuzdesi NUMERIC DEFAULT 0, ldipiskontotutari NUMERIC DEFAULT 0,
    nkdvorani1 NUMERIC DEFAULT 0, lkdvmatrahi1 NUMERIC DEFAULT 0, lkdv1 NUMERIC DEFAULT 0,
    nkdvorani2 NUMERIC DEFAULT 0, lkdvmatrahi2 NUMERIC DEFAULT 0, lkdv2 NUMERIC DEFAULT 0,
    nkdvorani3 NUMERIC DEFAULT 0, lkdvmatrahi3 NUMERIC DEFAULT 0, lkdv3 NUMERIC DEFAULT 0,
    nkdvorani4 NUMERIC DEFAULT 0, lkdvmatrahi4 NUMERIC DEFAULT 0, lkdv4 NUMERIC DEFAULT 0,
    nkdvorani5 NUMERIC DEFAULT 0, lkdvmatrahi5 NUMERIC DEFAULT 0, lkdv5 NUMERIC DEFAULT 0,
    lpesinat NUMERIC DEFAULT 0, nvadefarkiyuzdesi NUMERIC DEFAULT 0,
    nvadekdvorani NUMERIC DEFAULT 0, lvadekdvmatrahi NUMERIC DEFAULT 0, lvadekdv NUMERIC DEFAULT 0,
    lvadefarki NUMERIC DEFAULT 0, lnettutar NUMERIC DEFAULT 0, sharekettipi VARCHAR(20) DEFAULT '',
    bmuhasebeyeislendimi BOOLEAN DEFAULT FALSE, skullaniciadi VARCHAR(60) DEFAULT '', dtekayittarihi TIMESTAMP
);
CREATE TABLE tbodeme (
    nodemeid VARCHAR(20) PRIMARY KEY, nalisverisid VARCHAR(20) DEFAULT '', sodemesekli VARCHAR(4) DEFAULT '',
    nodemekodu NUMERIC DEFAULT 0, skasiyerrumuzu VARCHAR(4) DEFAULT '', dteodemetarihi TIMESTAMP,
    dtevalortarihi TIMESTAMP, lodemetutar NUMERIC DEFAULT 0, sdovizcinsi VARCHAR(3) DEFAULT 'TL',
    ldoviztutar NUMERIC DEFAULT 0, lmakbuzno NUMERIC DEFAULT 0, lodemeno NUMERIC DEFAULT 0,
    ntaksitid VARCHAR(20) DEFAULT '', niadealisverisid VARCHAR(20) DEFAULT '',
    bmuhasebeyeislendimi BOOLEAN DEFAULT FALSE, nkasano NUMERIC DEFAULT 0,
    skullaniciadi VARCHAR(60) DEFAULT '', dtekayittarihi TIMESTAMP, smagaza VARCHAR(4) DEFAULT ''
);
CREATE TABLE tbstokfisidetayi (
    nislemid NUMERIC NOT NULL, nstokid INTEGER DEFAULT 0, dteislemtarihi TIMESTAMP,
    nfirmaid INTEGER DEFAULT 0, nmusteriid INTEGER DEFAULT 0, sfistipi VARCHAR(3) DEFAULT '',
    dtefistarihi TIMESTAMP, lfisno NUMERIC DEFAULT 0, ngiriscikis NUMERIC DEFAULT 0,
    sdepo VARCHAR(4) DEFAULT '', lreyonfisno NUMERIC DEFAULT 0, sstokislem VARCHAR(3) DEFAULT '',
    skasiyerrumuzu VARCHAR(4) DEFAULT '', ssaticirumuzu VARCHAR(4) DEFAULT '', sodemekodu VARCHAR(4) DEFAULT '',
    dteirsaliyetarihi TIMESTAMP, lirsaliyeno NUMERIC DEFAULT 0,
    lgirismiktar1 NUMERIC DEFAULT 0, lgirismiktar2 NUMERIC DEFAULT 0,
    lgirisfiyat NUMERIC(19,4) DEFAULT 0, lgiristutar NUMERIC DEFAULT 0,
    lcikismiktar1 NUMERIC DEFAULT 0, lcikismiktar2 NUMERIC DEFAULT 0,
    lcikisfiyat NUMERIC(19,4) DEFAULT 0, lcikistutar NUMERIC DEFAULT 0,
    sfiyattipi VARCHAR(4) DEFAULT '', lbrutfiyat NUMERIC(19,4) DEFAULT 0, lbruttutar NUMERIC DEFAULT 0,
    lmaliyetfiyat NUMERIC(19,4) DEFAULT 0, lmaliyettutar NUMERIC DEFAULT 0,
    lilavemaliyettutar NUMERIC DEFAULT 0, niskontoyuzdesi NUMERIC DEFAULT 0, liskontotutari NUMERIC DEFAULT 0,
    sdovizcinsi VARCHAR(3) DEFAULT 'TL', ldovizfiyat NUMERIC(19,4) DEFAULT 0,
    nsiparisid INTEGER DEFAULT 0, nreceteno NUMERIC DEFAULT 0, ntransferid NUMERIC DEFAULT 0,
    stransferdepo VARCHAR(4) DEFAULT '', nkdvorani NUMERIC DEFAULT 0, nhesapid INTEGER DEFAULT 0,
    saciklama VARCHAR(60) DEFAULT '', sharekettipi VARCHAR(20) DEFAULT '',
    bmuhasebeyeislendimi BOOLEAN DEFAULT FALSE, skullaniciadi VARCHAR(60) DEFAULT '',
    dtekayittarihi TIMESTAMP, nalisverisid VARCHAR(20) DEFAULT '',
    nstokfisiid NUMERIC DEFAULT 0, nirsaliyefisiid NUMERIC DEFAULT 0
);
"""

MIGRATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_stok_kod ON tbstok (skodu);
CREATE INDEX IF NOT EXISTS idx_stok_aciklama ON tbstok (saciklama);
CREATE INDEX IF NOT EXISTS idx_barkod_barkod ON tbstokbarkodu (sbarkod);
CREATE INDEX IF NOT EXISTS idx_barkod_stokid ON tbstokbarkodu (nstokid);
CREATE INDEX IF NOT EXISTS idx_fiyat_stokid ON tbstokfiyati (nstokid, sfiyattipi);
CREATE INDEX IF NOT EXISTS idx_av_tarih ON tbalisveris (dtefaturatarihi);
CREATE INDEX IF NOT EXISTS idx_av_fistipi ON tbalisveris (sfistipi);
CREATE INDEX IF NOT EXISTS idx_sfd_tarih ON tbstokfisidetayi (dteislemtarihi);
CREATE INDEX IF NOT EXISTS idx_sfd_stokid ON tbstokfisidetayi (nstokid);
CREATE INDEX IF NOT EXISTS idx_sfd_avid ON tbstokfisidetayi (nalisverisid);
CREATE INDEX IF NOT EXISTS idx_odeme_avid ON tbodeme (nalisverisid);
"""

MIGRATE_SECRET = 'pos-migrate-2024'

@app.route('/api/migrate/init', methods=['POST'])
def api_migrate_init():
    if request.json.get('secret') != MIGRATE_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(MIGRATE_TABLES_SQL)
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'msg': 'Tablolar olusturuldu'})

@app.route('/api/migrate/data', methods=['POST'])
def api_migrate_data():
    if request.json.get('secret') != MIGRATE_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    table = request.json.get('table')
    rows = request.json.get('rows', [])
    cols = request.json.get('cols', [])
    if not table or not rows:
        return jsonify({'error': 'table ve rows gerekli'}), 400
    col_names = ', '.join([c.lower() for c in cols])
    placeholders = ', '.join(['%s'] * len(cols))
    sql = f'INSERT INTO {table.lower()} ({col_names}) VALUES ({placeholders})'
    conn = get_connection()
    cursor = conn.cursor()
    count = 0
    for row in rows:
        try:
            cursor.execute(sql, row)
            count += 1
        except Exception as e:
            conn.rollback()
            continue
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'inserted': count, 'total': len(rows)})

@app.route('/api/migrate/index', methods=['POST'])
def api_migrate_index():
    if request.json.get('secret') != MIGRATE_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(MIGRATE_INDEXES_SQL)
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'msg': 'Indexler olusturuldu'})


@app.route('/api/sync/max-id', methods=['POST'])
def api_sync_max_id():
    if request.json.get('secret') != MIGRATE_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    conn = get_connection()
    cursor = conn.cursor()
    result = {}
    tables = {
        'tbAlisVeris': 'dtekayittarihi',
        'tbOdeme': 'dtekayittarihi',
        'tbStokFisiDetayi': 'nislemid',
        'tbMusteri': 'nmusteriid',
    }
    for table, col in tables.items():
        try:
            cursor.execute(f'SELECT MAX({col}) FROM {table.lower()}')
            val = cursor.fetchone()[0]
            if val is not None:
                result[table] = val.isoformat() if hasattr(val, 'isoformat') else val
            else:
                result[table] = None
        except:
            result[table] = None
    conn.close()
    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
