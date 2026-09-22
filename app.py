from flask import Flask, render_template, request, jsonify, Response
from config import query, execute, get_connection, adapt_sql, DB_MODE
from datetime import datetime, date
import traceback
import threading
import requests as req
import os
import io

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
    result = query(
        "SELECT TOP 1 nAlisverisID FROM tbAlisVeris "
        "WHERE nAlisverisID LIKE ? "
        "ORDER BY CAST(SUBSTRING(nAlisverisID, 2, 8) AS INT) DESC",
        ['P%']
    )
    if result:
        num = int(''.join(c for c in result[0]['nAlisverisID'].strip() if c.isdigit()))
        return f"P{num + 1:08d}"
    return "P00000001"

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
    result = query(
        "SELECT TOP 1 nOdemeID FROM tbOdeme "
        "WHERE nOdemeID LIKE ? "
        "ORDER BY CAST(SUBSTRING(nOdemeID, 2, 8) AS INT) DESC",
        ['P%']
    )
    if result:
        num = int(''.join(c for c in result[0]['nOdemeID'].strip() if c.isdigit()))
        return f"P{num + 1:08d}"
    return "P00000001"

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


# --- STARTUP ---

def init_log_table():
    if DB_MODE != 'postgres':
        return
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS veresiye_odeme_log (
                id SERIAL PRIMARY KEY,
                kayit_tarihi TIMESTAMP DEFAULT NOW(),
                musteri_id INTEGER,
                musteri_adi VARCHAR(200),
                odeme_tutari NUMERIC(12,2),
                kapanan_sayisi INTEGER
            )
        """)
        conn.commit()
        conn.close()
    except Exception:
        pass

init_log_table()


# --- ROUTES ---

@app.route('/healthz')
def healthz():
    return 'ok'

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
            adapt_sql("INSERT INTO tbStok (nStokID, sKodu, sAciklama, sKisaAdi, nStokTipi, "
            "sBirimCinsi1, nIskontoYuzdesi, sKdvTipi, nTeminSuresi, "
            "lAsgariMiktar, lAzamiMiktar, sOzelNot, nFiyatlandirma, sModel, "
            "sKullaniciAdi, dteKayitTarihi, bEksiyeDusulebilirmi, sDefaultAsortiTipi, "
            "bEksideUyarsinmi, bOTVVar, sOTVTipi, nIskontoYuzdesiAV, bEk1, nEk2, "
            "nPrim, nEn, nBoy, nYukseklik, nHacim, nAgirlik, sDovizCinsi, "
            "sAlisKdvTipi, nButce, nKarlilik, sUlke) "
            "VALUES (?, ?, ?, ?, 0, ?, 0, ?, 0, 0, 0, '', 0, '', "
            "'POS', ?, 0, '', 0, 0, '', 0, 0, 0, 0, 0, 0, 0, 0, 0, 'TL', ?, 0, 0, '')"),
            [stok_id, urun_kodu, urun_adi, kisa_adi, birim, kdv_tipi, now, kdv_tipi]
        )

        # tbStokBarkodu kaydı
        cursor.execute(
            adapt_sql("INSERT INTO tbStokBarkodu (nStokID, sBarkod, nFirmaID, sKarsiStokKodu, "
            "sKarsiStokAciklama, sBirimCinsi, lBirimMiktar) "
            "VALUES (?, ?, 0, '', '', ?, 0)"),
            [stok_id, barkod, birim]
        )

        # Tartili urun ise kisaltilmis barkodu da ekle (urun kodu kismi)
        parsed = parse_tabak_barkod(barkod)
        if parsed['tartili'] and parsed['urun_kodu'] != barkod:
            cursor.execute(
                adapt_sql("INSERT INTO tbStokBarkodu (nStokID, sBarkod, nFirmaID, sKarsiStokKodu, "
                "sKarsiStokAciklama, sBirimCinsi, lBirimMiktar) "
                "VALUES (?, ?, 0, '', '', ?, 0)"),
                [stok_id, parsed['urun_kodu'], birim]
            )

        # tbStokFiyati kaydı (satis fiyati)
        if fiyat > 0:
            cursor.execute(
                adapt_sql("INSERT INTO tbStokFiyati (nStokID, sFiyatTipi, lFiyat, "
                "dteFiyatTespitTarihi, sKullaniciAdi, dteKayitTarihi) "
                "VALUES (?, '1', ?, ?, 'POS', ?)"),
                [stok_id, fiyat, now, now]
            )

        # tbStokSinifi kaydı (bos sinif)
        cursor.execute(
            adapt_sql("INSERT INTO tbStokSinifi (nStokID, sSinifKodu1, sSinifKodu2, sSinifKodu3, "
            "sSinifKodu4, sSinifKodu5, sSinifKodu6, sSinifKodu7, sSinifKodu8, "
            "sSinifKodu9, sSinifKodu10, sSinifKodu11, sSinifKodu12, sSinifKodu13, "
            "sSinifKodu14, sSinifKodu15) "
            "VALUES (?, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '')"),
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
            adapt_sql("INSERT INTO tbAlisVeris (nAlisverisID, sFisTipi, dteFaturaTarihi, "
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
            "0, 0, 0, 0, 0, 0, ?, '', ?, 'POS', ?)"),
            [alisveris_id, fis_tipi, now, fis_no, musteri_id,
             musteri_adi, musteri_soyadi, toplam_miktar, toplam_tutar,
             toplam_tutar, False, now]
        )

        islem_id = get_next_islem_id()
        for k in kalemler:
            miktar = float(k['miktar'])
            fiyat = float(k['fiyat'])
            tutar = miktar * fiyat
            cursor.execute(
                adapt_sql("INSERT INTO tbStokFisiDetayi (nIslemID, nStokID, dteIslemTarihi, "
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
                "'TL', 0, 0, 0, 0, '', 0, 0, '', '', ?, 'POS', ?, ?, 0, 0)"),
                [islem_id, k['stok_id'], now, musteri_id, fis_tipi, now,
                 fis_no, odeme_sekli, now, miktar, fiyat, tutar, False, now, alisveris_id]
            )
            islem_id += 1

        odeme_id = get_next_odeme_id()
        cursor.execute(
            adapt_sql("INSERT INTO tbOdeme (nOdemeID, nAlisverisID, sOdemeSekli, "
            "nOdemeKodu, sKasiyerRumuzu, dteOdemeTarihi, dteValorTarihi, "
            "lOdemeTutar, sDovizCinsi, lDovizTutar, lMakbuzNo, lOdemeNo, "
            "nTaksitID, nIadeAlisverisID, bMuhasebeyeIslendimi, nKasaNo, "
            "sKullaniciAdi, dteKayitTarihi, sMagaza) "
            "VALUES (?, ?, ?, 0, '', ?, ?, ?, 'TL', 0, 0, 0, '', '', ?, 1, "
            "'POS', ?, 'D001')"),
            [odeme_id, alisveris_id, odeme_sekli, now, now, toplam_tutar, False, now]
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
            "SELECT TOP 50 nMusteriID, sAdi, sSoyadi, sGSM, sIsIl "
            "FROM tbMusteri WHERE sAdi LIKE ? OR sSoyadi LIKE ? OR sGSM LIKE ? "
            "ORDER BY sAdi",
            [f'%{search}%', f'%{search}%', f'%{search}%']
        )
    else:
        rows = query(
            "SELECT TOP 50 nMusteriID, sAdi, sSoyadi, sGSM, sIsIl "
            "FROM tbMusteri ORDER BY sAdi"
        )
    return jsonify([{
        'id': r['nMusteriID'],
        'adi': (r['sAdi'] or '').strip(),
        'soyadi': (r['sSoyadi'] or '').strip(),
        'telefon': (r['sGSM'] or '').strip(),
        'il': (r['sIsIl'] or '').strip(),
    } for r in rows])


@app.route('/api/musteri/ekle', methods=['POST'])
def api_musteri_ekle():
    d = request.json or {}
    adi = (d.get('adi') or '').strip()
    soyadi = (d.get('soyadi') or '').strip()
    if not adi and not soyadi:
        return jsonify({'error': 'Ad veya soyad gerekli'}), 400
    telefon = (d.get('telefon') or '').strip()
    il = (d.get('il') or '').strip()

    max_id = query("SELECT ISNULL(MAX(nMusteriID), 0) AS maxid FROM tbMusteri")
    new_id = int(max_id[0]['maxid']) + 1

    execute(
        adapt_sql("INSERT INTO tbMusteri (nMusteriID, sAdi, sSoyadi, sGSM, sIsIl) VALUES (?, ?, ?, ?, ?)"),
        [new_id, adi, soyadi, telefon, il]
    )
    return jsonify({'ok': True, 'id': new_id})


@app.route('/api/musteri/<int:musteri_id>/guncelle', methods=['PUT'])
def api_musteri_guncelle(musteri_id):
    d = request.json or {}
    adi = (d.get('adi') or '').strip()
    soyadi = (d.get('soyadi') or '').strip()
    if not adi and not soyadi:
        return jsonify({'error': 'Ad veya soyad gerekli'}), 400
    telefon = (d.get('telefon') or '').strip()
    il = (d.get('il') or '').strip()

    execute(
        adapt_sql("UPDATE tbMusteri SET sAdi=?, sSoyadi=?, sGSM=?, sIsIl=? WHERE nMusteriID=?"),
        [adi, soyadi, telefon, il, musteri_id]
    )
    return jsonify({'ok': True})


# --- API: MUSTERI GECMISI ---

@app.route('/api/musteri/<int:musteri_id>/gecmis')
def api_musteri_gecmis(musteri_id):
    # Ozet
    ozet = query(
        "SELECT COUNT(*) AS islem_adedi, "
        "ISNULL(SUM(a.lNetTutar), 0) AS toplam_harcama, "
        "ISNULL(AVG(a.lNetTutar), 0) AS ort_fis, "
        "MIN(a.dteFaturaTarihi) AS ilk_alisveris, "
        "MAX(a.dteFaturaTarihi) AS son_alisveris "
        "FROM tbAlisVeris a "
        "WHERE a.nMusteriID = ? AND a.lNetTutar < 10000000",
        [musteri_id]
    )
    # Odeme tipi dagilimi
    odeme_rows = query(
        "SELECT RTRIM(o.sOdemeSekli) AS sekil, "
        "COUNT(*) AS islem_adedi, ISNULL(SUM(a.lNetTutar), 0) AS toplam "
        "FROM tbAlisVeris a "
        "JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "WHERE a.nMusteriID = ? AND a.lNetTutar < 10000000 "
        "GROUP BY RTRIM(o.sOdemeSekli)",
        [musteri_id]
    )
    odeme = {}
    for r in odeme_rows:
        odeme[(r['sekil'] or '').strip()] = {
            'islem': int(r['islem_adedi']), 'tutar': float(r['toplam'])
        }
    # Son 50 alisveris
    satislar = query(
        "SELECT TOP 50 a.nAlisverisID, a.lFaturaNo, a.dteFaturaTarihi, "
        "a.dteKayitTarihi, a.lNetTutar, a.lToplamMiktar, "
        "RTRIM(ISNULL(o.sOdemeSekli, '')) AS odeme_sekli "
        "FROM tbAlisVeris a "
        "LEFT JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "WHERE a.nMusteriID = ? AND a.lNetTutar < 10000000 "
        "ORDER BY a.dteKayitTarihi DESC",
        [musteri_id]
    )
    # Veresiye borç durumu
    veresiye_rows = query(
        "SELECT ISNULL(SUM(a.lNetTutar), 0) AS veresiye_borc, COUNT(*) AS veresiye_bekleyen "
        "FROM tbAlisVeris a "
        "JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "WHERE a.nMusteriID = ? AND RTRIM(o.sOdemeSekli) = 'V' AND a.lNetTutar < 10000000",
        [musteri_id]
    )
    veresiye_borc = float(veresiye_rows[0]['veresiye_borc']) if veresiye_rows else 0
    veresiye_bekleyen = int(veresiye_rows[0]['veresiye_bekleyen']) if veresiye_rows else 0

    o = ozet[0]
    return jsonify({
        'ozet': {
            'islem_adedi': int(o['islem_adedi']),
            'toplam_harcama': float(o['toplam_harcama']),
            'ort_fis': float(o['ort_fis']),
            'ilk': o['ilk_alisveris'].strftime('%d.%m.%Y') if o['ilk_alisveris'] else '',
            'son': o['son_alisveris'].strftime('%d.%m.%Y') if o['son_alisveris'] else '',
            'veresiye_borc': veresiye_borc,
            'veresiye_bekleyen': veresiye_bekleyen,
        },
        'odeme': odeme,
        'satislar': [{
            'id': r['nAlisverisID'].strip(),
            'fis_no': int(r['lFaturaNo']),
            'tarih': r['dteFaturaTarihi'].strftime('%d.%m.%Y') if r['dteFaturaTarihi'] else '',
            'saat': r['dteKayitTarihi'].strftime('%H:%M') if r['dteKayitTarihi'] else '',
            'tutar': float(r['lNetTutar']),
            'miktar': float(r['lToplamMiktar']),
            'odeme': (r['odeme_sekli'] or '').strip(),
        } for r in satislar],
    })


@app.route('/api/musteri/<int:musteri_id>/aylik')
def api_musteri_aylik(musteri_id):
    rows = query(
        adapt_sql(
            "SELECT YEAR(a.dteFaturaTarihi) AS yil, MONTH(a.dteFaturaTarihi) AS ay, "
            "ISNULL(SUM(a.lNetTutar), 0) AS ciro, COUNT(*) AS islem "
            "FROM tbAlisVeris a "
            "WHERE a.nMusteriID = ? AND a.lNetTutar < 10000000 "
            "AND a.dteFaturaTarihi >= DATEADD(month, -11, DATEADD(day, 1-DAY(GETDATE()), GETDATE())) "
            "GROUP BY YEAR(a.dteFaturaTarihi), MONTH(a.dteFaturaTarihi) "
            "ORDER BY yil, ay"
        ),
        [musteri_id]
    )
    ay_adlari = ['', 'Oca', 'Sub', 'Mar', 'Nis', 'May', 'Haz', 'Tem', 'Agu', 'Eyl', 'Eki', 'Kas', 'Ara']
    return jsonify([{
        'etiket': ay_adlari[int(r['ay'])] + ' ' + str(int(r['yil']))[2:],
        'ciro': float(r['ciro']),
        'islem': int(r['islem']),
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
        "a.dteKayitTarihi, a.lFaturaNo, a.sAlisverisYapanAdi, a.sAlisverisYapanSoyadi, "
        "a.lToplamMiktar, a.lNetTutar, a.sKullaniciAdi "
        "FROM tbAlisVeris a WHERE a.lNetTutar < 10000000 "
        "ORDER BY a.dteFaturaTarihi DESC, a.lFaturaNo DESC"
    )
    return jsonify([{
        'id': r['nAlisverisID'].strip(),
        'fis_tipi': (r['sFisTipi'] or '').strip(),
        'tarih': r['dteFaturaTarihi'].strftime('%d.%m.%Y') if r['dteFaturaTarihi'] else '',
        'saat': r['dteKayitTarihi'].strftime('%H:%M') if r['dteKayitTarihi'] else '',
        'fis_no': int(r['lFaturaNo']),
        'musteri': f"{(r['sAlisverisYapanAdi'] or '').strip()} {(r['sAlisverisYapanSoyadi'] or '').strip()}".strip(),
        'miktar': float(r['lToplamMiktar']),
        'tutar': float(r['lNetTutar']),
    } for r in rows])

@app.route('/api/rapor/satis_detay')
def api_rapor_satis_detay():
    alisveris_id = request.args.get('id', '')
    rows = query(
        "SELECT d.lCikisMiktar1, d.lCikisFiyat, d.lCikisTutar, s.sAciklama "
        "FROM tbStokFisiDetayi d "
        "JOIN tbStok s ON d.nStokID = s.nStokID "
        "WHERE d.nAlisverisID = ? AND d.nGirisCikis = 3",
        [alisveris_id]
    )
    return jsonify([{
        'urun': (r['sAciklama'] or '').strip(),
        'miktar': float(r['lCikisMiktar1']),
        'fiyat': float(r['lCikisFiyat']),
        'tutar': float(r['lCikisTutar']),
    } for r in rows])


# --- API: KARSILASTIRMALI RAPOR ---

@app.route('/karsilastirma')
def karsilastirma():
    return render_template('karsilastirma.html')

@app.route('/api/rapor/karsilastirma')
def api_rapor_karsilastirma():
    from datetime import timedelta
    tip = request.args.get('tip', 'hafta')
    bugun = date.today()

    if tip == 'hafta':
        # Bu hafta: Pazartesi - bugun
        haftanin_gunu = bugun.weekday()  # 0=Pzt
        bu_baslangic = bugun - timedelta(days=haftanin_gunu)
        bu_bitis = bugun
        g_baslangic = bu_baslangic - timedelta(days=7)
        g_bitis = bu_bitis - timedelta(days=7)
        etiket_fmt = '%a'  # Pzt, Sal...
    else:
        # Bu ay: 1 - bugun
        bu_baslangic = bugun.replace(day=1)
        bu_bitis = bugun
        from calendar import monthrange
        onceki_ay = bugun.month - 1 or 12
        onceki_yil = bugun.year if bugun.month > 1 else bugun.year - 1
        gun_sayisi = monthrange(onceki_yil, onceki_ay)[1]
        g_baslangic = date(onceki_yil, onceki_ay, 1)
        g_bitis = date(onceki_yil, onceki_ay, min(bugun.day, gun_sayisi))
        etiket_fmt = '%d'

    def get_gunluk(baslangic, bitis):
        rows = query(
            "SELECT CAST(dteFaturaTarihi AS DATE) AS gun, "
            "COUNT(*) AS islem_adedi, ISNULL(SUM(lNetTutar), 0) AS ciro "
            "FROM tbAlisVeris "
            "WHERE CAST(dteFaturaTarihi AS DATE) >= ? "
            "AND CAST(dteFaturaTarihi AS DATE) <= ? "
            "AND lNetTutar < 10000000 "
            "GROUP BY CAST(dteFaturaTarihi AS DATE) "
            "ORDER BY gun",
            [baslangic.isoformat(), bitis.isoformat()]
        )
        # Her gun icin bos satirlarla doldur
        gun_map = {}
        for r in rows:
            g = r['gun'] if hasattr(r['gun'], 'strftime') else date.fromisoformat(str(r['gun']))
            gun_map[g] = {'ciro': float(r['ciro']), 'islem': int(r['islem_adedi'])}

        result = []
        current = baslangic
        while current <= bitis:
            d = gun_map.get(current, {'ciro': 0, 'islem': 0})
            result.append({
                'tarih': current.isoformat(),
                'etiket': current.strftime(etiket_fmt),
                'ciro': d['ciro'],
                'islem': d['islem'],
            })
            current += timedelta(days=1)
        return result

    return jsonify({
        'tip': tip,
        'bu_donem': get_gunluk(bu_baslangic, bu_bitis),
        'gecen_donem': get_gunluk(g_baslangic, g_bitis),
        'bu_baslangic': bu_baslangic.isoformat(),
        'bu_bitis': bu_bitis.isoformat(),
        'g_baslangic': g_baslangic.isoformat(),
        'g_bitis': g_bitis.isoformat(),
    })


# --- API: KASA RAPORU ---

@app.route('/kasa-raporu')
def kasa_raporu():
    return render_template('kasa_raporu.html')

@app.route('/alis-faturasi')
def alis_faturasi():
    return render_template('alis_faturasi.html')


@app.route('/api/alis-faturasi/tedarikciler')
def api_alis_faturasi_tedarikciler():
    rows = query(
        "SELECT DISTINCT m.nFirmaID, ISNULL(f.sAciklama, '') AS ad "
        "FROM tbStokFisiMaster m "
        "LEFT JOIN tbFirma f ON f.nFirmaID = m.nFirmaID "
        "WHERE m.sFisTipi = 'FA' AND m.nGirisCikis = 1 "
        "ORDER BY ad"
    )
    return jsonify([{
        'firma_id': int(r['nFirmaID']),
        'ad': (r.get('ad') or '').strip(),
    } for r in rows if (r.get('ad') or '').strip()])


@app.route('/api/alis-faturasi/liste')
def api_alis_faturasi_liste():
    firma_id = request.args.get('firma_id', type=int)
    bas = request.args.get('bas', '')
    bit = request.args.get('bit', '')
    filtres = []
    params = []
    if firma_id:
        filtres.append("m.nFirmaID = ?")
        params.append(firma_id)
    if bas:
        filtres.append("m.dteFisTarihi >= ?")
        params.append(bas)
    if bit:
        from datetime import date, timedelta
        bit_dt = date.fromisoformat(bit) + timedelta(days=1)
        filtres.append("m.dteFisTarihi < ?")
        params.append(bit_dt.isoformat())
    filtre = ("AND " + " AND ".join(filtres)) if filtres else ""
    rows = query(
        "SELECT TOP 200 m.nStokFisiID, m.dteFisTarihi AS tarih, "
        "m.lFisNo AS fis_no, m.lNetTutar AS toplam, "
        "ISNULL(f.sAciklama, '') AS tedarikci, "
        "ISNULL(d.satir_sayisi, 0) AS satir_sayisi "
        "FROM tbStokFisiMaster m "
        "LEFT JOIN tbFirma f ON f.nFirmaID = m.nFirmaID "
        "LEFT JOIN ("
        "  SELECT nStokFisiID, COUNT(*) AS satir_sayisi "
        "  FROM tbStokFisiDetayi WHERE nGirisCikis = 1 GROUP BY nStokFisiID"
        ") d ON d.nStokFisiID = m.nStokFisiID "
        f"WHERE m.sFisTipi = 'FA' AND m.nGirisCikis = 1 {filtre} "
        "ORDER BY m.dteFisTarihi DESC, m.nStokFisiID DESC",
        params
    )
    return jsonify([{
        'fis_id': int(r['nStokFisiID']),
        'tarih': r['tarih'].strftime('%d.%m.%Y') if r['tarih'] else '',
        'satir': int(r['satir_sayisi']),
        'toplam': float(r['toplam']),
        'fis_no': int(r['fis_no'] or 0),
        'tedarikci': (r.get('tedarikci') or '').strip(),
    } for r in rows])


@app.route('/api/alis-faturasi/<int:fis_id>')
def api_alis_faturasi_detay(fis_id):
    rows = query(
        "SELECT d.nIslemID, d.nStokID, s.sAciklama, s.sBirimCinsi1, "
        "d.lGirisMiktar1, d.lGirisFiyat, d.lGirisTutar, d.dteIslemTarihi "
        "FROM tbStokFisiDetayi d "
        "JOIN tbStok s ON d.nStokID = s.nStokID "
        "WHERE d.nStokFisiID = ? AND d.nGirisCikis = 1 "
        "ORDER BY d.nIslemID",
        [fis_id]
    )
    return jsonify([{
        'islem_id': int(r['nIslemID']),
        'stok_id': int(r['nStokID']),
        'urun': (r['sAciklama'] or '').strip(),
        'birim': (r['sBirimCinsi1'] or '').strip(),
        'miktar': float(r['lGirisMiktar1']),
        'fiyat': float(r['lGirisFiyat']),
        'tutar': float(r['lGirisTutar']),
        'tarih': r['dteIslemTarihi'].strftime('%d.%m.%Y') if r['dteIslemTarihi'] else '',
    } for r in rows])


@app.route('/api/alis-faturasi/kaydet', methods=['POST'])
def api_alis_faturasi_kaydet():
    d = request.json or {}
    tarih_str = d.get('tarih', date.today().isoformat())
    fis_no = int(d.get('fis_no') or 1)
    satirlar = d.get('satirlar', [])

    if not satirlar:
        return jsonify({'error': 'En az bir urun gerekli'}), 400

    try:
        tarih = date.fromisoformat(tarih_str)
    except Exception:
        return jsonify({'error': 'Gecersiz tarih'}), 400

    now = datetime.now()
    tarih_dt = datetime(tarih.year, tarih.month, tarih.day)

    # Birim bilgilerini toplu al
    stok_ids = [int(s['stok_id']) for s in satirlar]
    placeholders = ','.join(['?' for _ in stok_ids])
    stok_rows = query(
        f"SELECT nStokID, sBirimCinsi1 FROM tbStok WHERE nStokID IN ({placeholders})",
        stok_ids
    )
    birim_map = {int(r['nStokID']): (r['sBirimCinsi1'] or '').strip() for r in stok_rows}

    toplam_miktar = sum(float(s['miktar']) for s in satirlar)
    toplam_tutar = sum(round(float(s['miktar']) * float(s['fiyat']), 2) for s in satirlar)

    master_sql = adapt_sql(
        "INSERT INTO tbStokFisiMaster "
        "(sFisTipi, dteFisTarihi, nGirisCikis, lFisNo, nFirmaID, sDepo, "
        "dteValorTarihi, bPesinmi, bListelendimi, bHizmetFaturasimi, "
        "lToplamMiktar, lMalBedeli, lMalIskontoTutari, "
        "nDipIskontoYuzdesi1, lDipIskontoTutari1, nDipIskontoYuzdesi2, "
        "lDipIskontoTutari2, lDipIskontoTutari3, "
        "lEkmaliyet1, lEkmaliyet2, lEkmaliyet3, "
        "nKdvOrani1, lKdvMatrahi1, lKdv1, "
        "nKdvOrani2, lKdvMatrahi2, lKdv2, "
        "nKdvOrani3, lKdvMatrahi3, lKdv3, "
        "nKdvOrani4, lKdvMatrahi4, lKdv4, "
        "nKdvOrani5, lKdvMatrahi5, lKdv5, "
        "lNetTutar, nTevkifatKdvOrani, lTevkifatKdvMatrahi, lTevkifatKdv, "
        "sHareketTipi, bMuhasebeyeIslendimi, bFisTamamlandimi, "
        "lTransferFisiID, sTransferDepo, bFaturayaDonustumu, "
        "sKullaniciAdi, dteKayitTarihi, sYaziIle, "
        "nOTVOrani1, lOTVMatrahi1, lOTV1, nOTVOrani2, lOTVMatrahi2, lOTV2, "
        "bKilitli, bEfatura, sEfaturaTipi, sEfaturaGuid, nEfaturaDurum) "
        "VALUES ('FA',?,1,?,1003,'D001',"
        "?,0,0,0,"
        "?,?,0,"
        "0,0,0,0,0,"
        "0,0,0,"
        "1,?,0,"
        "0,0,0,"
        "0,0,0,"
        "0,0,0,"
        "0,0,0,"
        "?,0,0,0,"
        "'001',0,1,"
        "0,'',0,"
        "'POS',?,'', "
        "0,?,0,0,0,0,"
        "0,0,'','',0)"
    )
    master_params = [tarih_dt, fis_no, tarih_dt, toplam_miktar, toplam_tutar, toplam_tutar, toplam_tutar, now, toplam_tutar]

    detay_sql = adapt_sql(
        "INSERT INTO tbStokFisiDetayi ("
        "nStokID, dteIslemTarihi, nFirmaID, nMusteriID, "
        "sFisTipi, dteFisTarihi, lFisNo, nGirisCikis, sDepo, "
        "lReyonFisNo, sStokIslem, sKasiyerRumuzu, sSaticiRumuzu, sOdemeKodu, "
        "dteIrsaliyeTarihi, lIrsaliyeNo, "
        "lGirisMiktar1, lGirisMiktar2, lGirisFiyat, lGirisTutar, "
        "lCikisMiktar1, lCikisMiktar2, lCikisFiyat, lCikisTutar, "
        "sFiyatTipi, lBrutFiyat, lBrutTutar, lMaliyetFiyat, lMaliyetTutar, "
        "lIlaveMaliyetTutar, nIskontoYuzdesi, lIskontoTutari, "
        "sDovizCinsi, lDovizFiyat, nReceteNo, "
        "nKdvOrani, nHesapID, sAciklama, sHareketTipi, "
        "bMuhasebeyeIslendimi, sKullaniciAdi, dteKayitTarihi, "
        "sDovizCinsi1, lDovizMiktari1, lDovizKuru1, "
        "sDovizCinsi2, lDovizMiktari2, lDovizKuru2, "
        "nOTVOrani, nStokFisiID, sHangiUygulama, sMasrafYontemi, sBirimCinsi, lBirimMiktar, "
        "nEkSaha1, nEkSaha2, bEkSoru1, bEkSoru2, nPrim, lPrimTutari, "
        "sSonKullaniciAdi, dteSonUpdateTarihi) "
        "VALUES (?,?,1003,0,'FA',?,?,1,'D001',"
        "0,'','','','',"
        "?,0,"
        "?,?,?,?,"
        "0,0,0,0,"
        "'A',?,?,?,?,"
        "0,0,0,"
        "'',?,0,"
        "1,0,'','001',"
        "0,'POS',?,"
        "'',0,0,"
        "'',0,0,"
        "0,?,'FA','',?,1,"
        "0,0,0,0,0,0,'POS',?)"
    )

    # Tum INSERT'leri tek connection'da yap
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(master_sql, master_params)
        cursor.execute("SELECT @@IDENTITY AS id")
        row = cursor.fetchone()
        if row is None or row[0] is None:
            raise ValueError("tbStokFisiMaster INSERT sonrasi ID alinamadi")
        yeni_fis_id = int(row[0])
        for satir in satirlar:
            stok_id = int(satir['stok_id'])
            miktar = float(satir['miktar'])
            fiyat = float(satir['fiyat'])
            tutar = round(miktar * fiyat, 2)
            birim = birim_map.get(stok_id, 'AD')
            cursor.execute(detay_sql, [
                stok_id, tarih_dt,
                tarih_dt, fis_no,
                tarih_dt,
                miktar, miktar, fiyat, tutar,
                fiyat, tutar, fiyat, tutar,
                fiyat,
                now,
                yeni_fis_id, birim,
                now,
            ])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return jsonify({'ok': True, 'fis_id': yeni_fis_id, 'satir_sayisi': len(satirlar)})


@app.route('/gun-sonu')
def gun_sonu():
    return render_template('gun_sonu.html')

@app.route('/api/rapor/kasa')
def api_rapor_kasa():
    tarih = request.args.get('tarih', date.today().isoformat())

    # Odeme tipine gore ozet
    ozet_rows = query(
        "SELECT RTRIM(o.sOdemeSekli) AS sekil, "
        "COUNT(*) AS islem_adedi, ISNULL(SUM(a.lNetTutar), 0) AS toplam "
        "FROM tbAlisVeris a "
        "JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "WHERE CAST(a.dteFaturaTarihi AS DATE) = ? "
        "AND a.lNetTutar < 10000000 "
        "GROUP BY RTRIM(o.sOdemeSekli)",
        [tarih]
    )

    ozet = {'N': {'tutar': 0, 'islem': 0}, 'K': {'tutar': 0, 'islem': 0},
            'V': {'tutar': 0, 'islem': 0}, 'T': {'tutar': 0, 'islem': 0}}
    for r in ozet_rows:
        s = (r['sekil'] or '').strip().upper()
        if s not in ozet:
            ozet[s] = {'tutar': 0, 'islem': 0}
        ozet[s]['tutar'] += float(r['toplam'])
        ozet[s]['islem'] += int(r['islem_adedi'])

    toplam_tutar = sum(v['tutar'] for v in ozet.values())
    toplam_islem = sum(v['islem'] for v in ozet.values())

    # Kasiyere gore breakdown
    kasiyer_rows = query(
        "SELECT ISNULL(k.sAdi, RTRIM(a.sKasiyerRumuzu)) AS eleman_adi, "
        "RTRIM(o.sOdemeSekli) AS sekil, "
        "COUNT(*) AS islem_adedi, ISNULL(SUM(a.lNetTutar), 0) AS toplam "
        "FROM tbAlisVeris a "
        "JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "LEFT JOIN tbKasiyer k ON RTRIM(a.sKasiyerRumuzu) = RTRIM(k.sKasiyerRumuzu) "
        "WHERE CAST(a.dteFaturaTarihi AS DATE) = ? "
        "AND a.lNetTutar < 10000000 "
        "GROUP BY ISNULL(k.sAdi, RTRIM(a.sKasiyerRumuzu)), RTRIM(o.sOdemeSekli) "
        "ORDER BY eleman_adi",
        [tarih]
    )

    kasiyerler = {}
    for r in kasiyer_rows:
        ad = (r['eleman_adi'] or '').strip() or 'Bilinmiyor'
        s = (r['sekil'] or '').strip().upper()
        if ad not in kasiyerler:
            kasiyerler[ad] = {'ad': ad, 'nakit': 0, 'kart': 0, 'veresiye': 0, 'diger': 0, 'islem': 0}
        tutar = float(r['toplam'])
        kasiyerler[ad]['islem'] += int(r['islem_adedi'])
        if s == 'N': kasiyerler[ad]['nakit'] += tutar
        elif s in ('K', '1'): kasiyerler[ad]['kart'] += tutar
        elif s in ('V', 'T'): kasiyerler[ad]['veresiye'] += tutar
        else: kasiyerler[ad]['diger'] += tutar

    for k in kasiyerler.values():
        k['toplam'] = k['nakit'] + k['kart'] + k['veresiye'] + k['diger']

    # Saatlik dagilim
    saat_rows = query(
        "SELECT DATEPART(HOUR, a.dteKayitTarihi) AS saat, "
        "COUNT(*) AS islem_adedi, ISNULL(SUM(a.lNetTutar), 0) AS toplam "
        "FROM tbAlisVeris a "
        "WHERE CAST(a.dteFaturaTarihi AS DATE) = ? "
        "AND a.lNetTutar < 10000000 "
        "GROUP BY DATEPART(HOUR, a.dteKayitTarihi) "
        "ORDER BY saat",
        [tarih]
    )

    return jsonify({
        'tarih': tarih,
        'ozet': {
            'nakit': ozet['N'],
            'kart': ozet['K'],
            'veresiye': ozet.get('V', {'tutar': 0, 'islem': 0}),
            'toplam': {'tutar': toplam_tutar, 'islem': toplam_islem},
        },
        'kasiyerler': sorted(kasiyerler.values(), key=lambda x: -x['toplam']),
        'saatlik': [{'saat': int(r['saat'] or 0), 'islem': int(r['islem_adedi']), 'tutar': float(r['toplam'])} for r in saat_rows],
    })


# --- API: AYLIK KARLILIK TRENDI ---

@app.route('/karlilik')
def karlilik():
    return render_template('karlilik.html')

@app.route('/api/rapor/karlilik')
def api_rapor_karlilik():
    rows = query(
        "SELECT YEAR(dteFaturaTarihi) AS yil, MONTH(dteFaturaTarihi) AS ay, "
        "COUNT(*) AS islem_adedi, "
        "ISNULL(SUM(lNetTutar), 0) AS ciro, "
        "ISNULL(SUM(lMalBedeli), 0) AS maliyet "
        "FROM tbAlisVeris "
        "WHERE dteFaturaTarihi >= DATEADD(MONTH, -12, GETDATE()) "
        "AND lNetTutar > 0 AND lNetTutar < 10000000 "
        "GROUP BY YEAR(dteFaturaTarihi), MONTH(dteFaturaTarihi) "
        "ORDER BY yil, ay"
    )

    ay_adlari = ['', 'Oca', 'Sub', 'Mar', 'Nis', 'May', 'Haz',
                 'Tem', 'Agu', 'Eyl', 'Eki', 'Kas', 'Ara']

    aylar = []
    for r in rows:
        ciro = float(r['ciro'])
        maliyet = float(r['maliyet'])
        kar = ciro - maliyet
        marj = round(kar / ciro * 100, 1) if ciro > 0 else 0
        aylar.append({
            'etiket': f"{ay_adlari[int(r['ay'])]} {str(int(r['yil']))[2:]}",
            'yil': int(r['yil']),
            'ay': int(r['ay']),
            'islem': int(r['islem_adedi']),
            'ciro': round(ciro, 2),
            'maliyet': round(maliyet, 2),
            'kar': round(kar, 2),
            'marj': marj,
        })

    # Ay bazinda buyume
    for i in range(1, len(aylar)):
        onceki = aylar[i-1]['ciro']
        su_an = aylar[i]['ciro']
        aylar[i]['buyume'] = round((su_an - onceki) / onceki * 100, 1) if onceki > 0 else 0
    if aylar:
        aylar[0]['buyume'] = 0

    toplam_ciro = sum(a['ciro'] for a in aylar)
    toplam_kar = sum(a['kar'] for a in aylar)
    ort_marj = round(toplam_kar / toplam_ciro * 100, 1) if toplam_ciro > 0 else 0
    en_iyi = max(aylar, key=lambda x: x['ciro']) if aylar else None
    # lMalBedeli gercek maliyet mi? Ciroya esitse maliyet verisi yok demek
    toplam_maliyet = sum(a['maliyet'] for a in aylar)
    maliyet_var = (toplam_ciro > 0 and toplam_maliyet > 0
                   and abs(toplam_maliyet - toplam_ciro) / toplam_ciro > 0.01)

    return jsonify({
        'aylar': aylar,
        'ozet': {
            'toplam_ciro': round(toplam_ciro, 2),
            'toplam_kar': round(toplam_kar, 2),
            'ort_marj': ort_marj,
            'en_iyi_ay': en_iyi['etiket'] if en_iyi else '-',
            'en_iyi_ciro': en_iyi['ciro'] if en_iyi else 0,
            'maliyet_var': maliyet_var,
        }
    })


# --- API: FIYAT YONETIMI ---

@app.route('/fiyat-yonetimi')
def fiyat_yonetimi():
    return render_template('fiyat_yonetimi.html')

@app.route('/api/fiyat/liste')
def api_fiyat_liste():
    search = request.args.get('q', '')
    if search:
        s_upper = search.upper()
        rows = query(
            "SELECT s.nStokID, s.sKodu, s.sAciklama, s.sBirimCinsi1, "
            "ISNULL(f.lFiyat, 0) AS fiyat, f.dteFiyatTespitTarihi AS son_guncelleme "
            "FROM tbStok s "
            "LEFT JOIN tbStokFiyati f ON s.nStokID = f.nStokID AND f.sFiyatTipi = '1' "
            "WHERE UPPER(s.sAciklama) LIKE ? OR UPPER(s.sKodu) LIKE ? "
            "ORDER BY s.sAciklama",
            [f'%{s_upper}%', f'%{s_upper}%']
        )
    else:
        rows = query(
            "SELECT s.nStokID, s.sKodu, s.sAciklama, s.sBirimCinsi1, "
            "ISNULL(f.lFiyat, 0) AS fiyat, f.dteFiyatTespitTarihi AS son_guncelleme "
            "FROM tbStok s "
            "LEFT JOIN tbStokFiyati f ON s.nStokID = f.nStokID AND f.sFiyatTipi = '1' "
            "ORDER BY s.sAciklama"
        )
    return jsonify([{
        'id': r['nStokID'],
        'kod': (r['sKodu'] or '').strip(),
        'ad': (r['sAciklama'] or '').strip(),
        'birim': (r['sBirimCinsi1'] or 'AD').strip(),
        'fiyat': float(r['fiyat'] or 0),
        'son_guncelleme': r['son_guncelleme'].strftime('%d.%m.%Y') if r['son_guncelleme'] else '',
    } for r in rows])

@app.route('/api/fiyat/guncelle', methods=['POST'])
def api_fiyat_guncelle():
    d = request.json or {}
    stok_id = d.get('stok_id')
    try:
        yeni_fiyat = float(d.get('fiyat', 0))
    except (TypeError, ValueError):
        return jsonify({'error': 'Gecersiz fiyat'}), 400
    if not stok_id or yeni_fiyat <= 0:
        return jsonify({'error': 'Gecersiz veri'}), 400

    now = datetime.now()
    existing = query(
        "SELECT nStokID FROM tbStokFiyati WHERE nStokID = ? AND sFiyatTipi = '1'",
        [stok_id]
    )
    if existing:
        execute(
            adapt_sql("UPDATE tbStokFiyati SET lFiyat = ?, dteFiyatTespitTarihi = ?, "
                      "sKullaniciAdi = 'POS' WHERE nStokID = ? AND sFiyatTipi = '1'"),
            [yeni_fiyat, now, stok_id]
        )
    else:
        execute(
            adapt_sql("INSERT INTO tbStokFiyati (nStokID, sFiyatTipi, lFiyat, "
                      "dteFiyatTespitTarihi, sKullaniciAdi, dteKayitTarihi) "
                      "VALUES (?, '1', ?, ?, 'POS', ?)"),
            [stok_id, yeni_fiyat, now, now]
        )
    return jsonify({'ok': True, 'fiyat': yeni_fiyat})


# --- API: STOK DURUMU ---

@app.route('/stok-durumu')
def stok_durumu():
    return render_template('stok_durumu.html')

@app.route('/api/stok/durum')
def api_stok_durum():
    rows = query(
        "SELECT s.sAciklama, s.sKodu, s.sBirimCinsi1, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis=3 AND d.dteIslemTarihi>=DATEADD(DAY,-30,GETDATE()) AND d.lCikisMiktar1<100000 THEN d.lCikisMiktar1 ELSE 0 END),0) AS cikis_30, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis=3 AND d.dteIslemTarihi>=DATEADD(DAY,-30,GETDATE()) AND d.lCikisMiktar1<100000 THEN d.lCikisTutar ELSE 0 END),0) AS tutar_30, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis=3 AND CAST(d.dteIslemTarihi AS DATE)=CAST(GETDATE() AS DATE) AND d.lCikisMiktar1<100000 THEN d.lCikisMiktar1 ELSE 0 END),0) AS bugun_miktar, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis=3 AND CAST(d.dteIslemTarihi AS DATE)=CAST(GETDATE() AS DATE) AND d.lCikisMiktar1<100000 THEN d.lCikisTutar ELSE 0 END),0) AS bugun_tutar, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis=3 AND d.dteIslemTarihi>=DATEADD(DAY,-7,GETDATE()) AND d.lCikisMiktar1<100000 THEN d.lCikisMiktar1 ELSE 0 END),0)/7.0 AS ort_gunluk, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis=1 AND d.lGirisMiktar1<100000 THEN d.lGirisMiktar1 ELSE 0 END),0) - "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis=3 AND d.lCikisMiktar1<100000 THEN d.lCikisMiktar1 ELSE 0 END),0) AS net_stok "
        "FROM tbStok s "
        "JOIN tbStokFisiDetayi d ON s.nStokID = d.nStokID "
        "GROUP BY s.sAciklama, s.sKodu, s.sBirimCinsi1 "
        "HAVING SUM(CASE WHEN d.nGirisCikis=3 AND d.dteIslemTarihi>=DATEADD(DAY,-30,GETDATE()) AND d.lCikisMiktar1<100000 THEN d.lCikisMiktar1 ELSE 0 END) > 0 "
        "ORDER BY cikis_30 DESC"
    )

    result = []
    for r in rows:
        cikis = float(r['cikis_30'])
        tutar = float(r['tutar_30'])
        bugun_m = float(r['bugun_miktar'])
        bugun_t = float(r['bugun_tutar'])
        ort = float(r['ort_gunluk'])
        net = float(r['net_stok'])

        if bugun_m == 0 and ort > 0:
            durum = 'satilmadi'
        elif ort > 0 and bugun_m >= ort * 1.1:
            durum = 'hizli'
        elif ort > 0 and bugun_m < ort * 0.4:
            durum = 'yavas'
        else:
            durum = 'normal'

        result.append({
            'ad': (r['sAciklama'] or '').strip(),
            'kod': (r['sKodu'] or '').strip(),
            'birim': (r['sBirimCinsi1'] or 'AD').strip(),
            'cikis_30': round(cikis, 3),
            'tutar_30': round(tutar, 2),
            'bugun_miktar': round(bugun_m, 3),
            'bugun_tutar': round(bugun_t, 2),
            'ort_gunluk': round(ort, 3),
            'net_stok': round(net, 3),
            'durum': durum,
        })

    return jsonify(result)


@app.route('/api/export/stok_durumu')
def export_stok_durumu():
    rows = query(
        "SELECT s.sAciklama, s.sKodu, s.sBirimCinsi1, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis = 3 THEN d.lCikisMiktar1 ELSE 0 END), 0) AS cikis_30, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis = 3 THEN d.lCikisTutar ELSE 0 END), 0) AS tutar_30, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis = 3 AND CAST(d.dteIslemTarihi AS DATE) = CAST(GETDATE() AS DATE) "
        "    THEN d.lCikisMiktar1 ELSE 0 END), 0) AS bugun_miktar, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis = 3 AND CAST(d.dteIslemTarihi AS DATE) = CAST(GETDATE() AS DATE) "
        "    THEN d.lCikisTutar ELSE 0 END), 0) AS bugun_tutar, "
        "ISNULL(SUM(CASE WHEN d.nGirisCikis = 3 AND d.dteIslemTarihi >= DATEADD(DAY, -7, GETDATE()) "
        "    THEN d.lCikisMiktar1 ELSE 0 END), 0) / 7.0 AS ort_gunluk "
        "FROM tbStok s "
        "JOIN tbStokFisiDetayi d ON s.nStokID = d.nStokID "
        "WHERE d.dteIslemTarihi >= DATEADD(DAY, -30, GETDATE()) "
        "AND d.lCikisMiktar1 < 100000 AND d.nGirisCikis = 3 "
        "GROUP BY s.sAciklama, s.sKodu, s.sBirimCinsi1 "
        "ORDER BY cikis_30 DESC"
    )
    durum_ad = {'satilmadi': 'Bugun Satilmadi', 'hizli': 'Hizli', 'yavas': 'Yavas', 'normal': 'Normal'}
    satirlar = []
    for r in rows:
        cikis = float(r['cikis_30'])
        bugun_m = float(r['bugun_miktar'])
        ort = float(r['ort_gunluk'])
        if bugun_m == 0 and ort > 0: durum = 'satilmadi'
        elif ort > 0 and bugun_m >= ort * 1.1: durum = 'hizli'
        elif ort > 0 and bugun_m < ort * 0.4: durum = 'yavas'
        else: durum = 'normal'
        satirlar.append([
            (r['sAciklama'] or '').strip(), (r['sKodu'] or '').strip(),
            (r['sBirimCinsi1'] or 'AD').strip(),
            round(bugun_m, 3), round(float(r['bugun_tutar']), 2),
            round(ort, 3), round(cikis, 3), round(float(r['tutar_30']), 2),
            durum_ad.get(durum, durum),
        ])
    tarih = date.today().isoformat()
    buf = make_excel([{
        'baslik': f'Satis Hizi Raporu {tarih}',
        'sutunlar': ['Urun', 'Kod', 'Birim', 'Bugun Miktar', 'Bugun Tutar',
                     'Ort/Gun', '30G Miktar', '30G Tutar', 'Durum'],
        'satirlar': satirlar,
    }])
    return excel_response(buf, f'stok_durumu_{tarih}.xlsx')


# --- API: URUN BAZLI RAPOR ---

@app.route('/urun-rapor')
def urun_rapor():
    return render_template('urun_rapor.html')


# --- API: BORCLU MUSTERI TAKIP ---

@app.route('/borclu')
def borclu():
    return render_template('borclu.html')

@app.route('/api/borclu')
def api_borclu():
    rows = query(
        "SELECT m.nMusteriID, m.sAdi, m.sSoyadi, m.sGSM, "
        "COUNT(*) AS veresiye_sayisi, "
        "ISNULL(SUM(a.lNetTutar), 0) AS toplam_borc, "
        "MAX(a.dteKayitTarihi) AS son_islem, "
        "MIN(a.dteKayitTarihi) AS ilk_borc "
        "FROM tbAlisVeris a "
        "JOIN tbMusteri m ON a.nMusteriID = m.nMusteriID "
        "JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "WHERE RTRIM(o.sOdemeSekli) = 'V' "
        "AND a.lNetTutar < 10000000 AND a.nMusteriID > 0 "
        "GROUP BY m.nMusteriID, m.sAdi, m.sSoyadi, m.sGSM "
        "ORDER BY toplam_borc DESC"
    )
    now = datetime.now()
    result = []
    for r in rows:
        ilk = r['ilk_borc']
        gun = (now - ilk).days if ilk else 0
        if gun >= 30:
            yaslik = 'eski'
        elif gun >= 7:
            yaslik = 'orta'
        else:
            yaslik = 'yeni'
        result.append({
            'id': r['nMusteriID'],
            'adi': (r['sAdi'] or '').strip(),
            'soyadi': (r['sSoyadi'] or '').strip(),
            'telefon': (r['sGSM'] or '').strip(),
            'veresiye_sayisi': int(r['veresiye_sayisi']),
            'toplam_borc': float(r['toplam_borc']),
            'son_islem': r['son_islem'].strftime('%d.%m.%Y') if r['son_islem'] else '',
            'gun': gun,
            'yaslik': yaslik,
        })
    return jsonify(result)

@app.route('/api/borclu/<int:musteri_id>/odeme', methods=['POST'])
def api_borclu_odeme(musteri_id):
    tutar = float(request.json.get('tutar', 0))
    if tutar <= 0:
        return jsonify({'error': 'Gecersiz tutar'}), 400

    # Veresiye kayitlari en eskiden yeniye
    rows = query(
        "SELECT o.nOdemeID, a.lNetTutar FROM tbOdeme o "
        "JOIN tbAlisVeris a ON RTRIM(o.nAlisverisID) = RTRIM(a.nAlisverisID) "
        "WHERE a.nMusteriID = ? AND RTRIM(o.sOdemeSekli) = 'V' "
        "AND a.lNetTutar < 10000000 "
        "ORDER BY a.dteKayitTarihi ASC",
        [musteri_id]
    )
    if not rows:
        return jsonify({'error': 'Borclu kayit bulunamadi'}), 404

    # En eskiden baslayarak, tutari tamamen karsilayan kayitlari isaretle
    remaining = tutar
    odeme_ids = []
    for r in rows:
        if remaining <= 0:
            break
        kayit_tutari = float(r['lNetTutar'])
        if remaining >= kayit_tutari - 0.01:  # tam karsiliyorsa kapat
            odeme_ids.append(r['nOdemeID'])
            remaining -= kayit_tutari
        else:
            break  # yetmiyorsa dur

    conn = get_connection()
    cursor = conn.cursor()
    try:
        for oid in odeme_ids:
            cursor.execute(adapt_sql("UPDATE tbOdeme SET sOdemeSekli = 'T' WHERE RTRIM(nOdemeID) = ?"), [oid.strip() if hasattr(oid, 'strip') else oid])
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'error': str(e)}), 500
    conn.close()

    # Odeme log kaydi
    try:
        m = query("SELECT sAdi, sSoyadi FROM tbMusteri WHERE nMusteriID = ?", [musteri_id])
        musteri_adi = ((m[0]['sAdi'] or '') + ' ' + (m[0]['sSoyadi'] or '')).strip() if m else ''
        if DB_MODE == 'postgres':
            conn2 = get_connection()
            cur2 = conn2.cursor()
            cur2.execute(
                "INSERT INTO veresiye_odeme_log (musteri_id, musteri_adi, odeme_tutari, kapanan_sayisi) "
                "VALUES (%s, %s, %s, %s)",
                [musteri_id, musteri_adi, tutar, len(odeme_ids)]
            )
            conn2.commit()
            conn2.close()
    except Exception:
        pass  # Log hatasi ana islemi etkilemesin

    return jsonify({'ok': True, 'odendi_sayisi': len(odeme_ids)})

@app.route('/api/borclu/<int:musteri_id>')
def api_borclu_detay(musteri_id):
    rows = query(
        "SELECT a.nAlisverisID, a.lFaturaNo, a.dteFaturaTarihi, a.dteKayitTarihi, "
        "a.lNetTutar, a.lToplamMiktar, "
        "ISNULL(k.sAdi, a.sKasiyerRumuzu) AS eleman_adi "
        "FROM tbAlisVeris a "
        "JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "LEFT JOIN tbKasiyer k ON RTRIM(a.sKasiyerRumuzu) = RTRIM(k.sKasiyerRumuzu) "
        "WHERE a.nMusteriID = ? AND RTRIM(o.sOdemeSekli) = 'V' "
        "AND a.lNetTutar < 10000000 "
        "ORDER BY a.dteKayitTarihi DESC",
        [musteri_id]
    )
    return jsonify([{
        'id': r['nAlisverisID'].strip(),
        'fis_no': int(r['lFaturaNo']),
        'tarih': r['dteFaturaTarihi'].strftime('%d.%m.%Y') if r['dteFaturaTarihi'] else '',
        'saat': r['dteKayitTarihi'].strftime('%H:%M') if r['dteKayitTarihi'] else '',
        'tutar': float(r['lNetTutar']),
        'miktar': float(r['lToplamMiktar']),
        'eleman': (r['eleman_adi'] or '').strip(),
    } for r in rows])

@app.route('/veresiye-gecmis')
def veresiye_gecmis():
    return render_template('veresiye_gecmis.html')

@app.route('/api/veresiye/gecmis')
def api_veresiye_gecmis():
    if DB_MODE != 'postgres':
        return jsonify([])
    musteri_id = request.args.get('musteri_id', type=int)
    bas = request.args.get('baslangic', '')
    bit = request.args.get('bitis', '')

    sql = ("SELECT id, kayit_tarihi, musteri_id, musteri_adi, odeme_tutari, kapanan_sayisi "
           "FROM veresiye_odeme_log ")
    params = []
    conditions = []
    if musteri_id:
        conditions.append("musteri_id = %s")
        params.append(musteri_id)
    if bas:
        conditions.append("kayit_tarihi::date >= %s")
        params.append(bas)
    if bit:
        conditions.append("kayit_tarihi::date <= %s")
        params.append(bit)
    if conditions:
        sql += "WHERE " + " AND ".join(conditions) + " "
    sql += "ORDER BY kayit_tarihi DESC LIMIT 500"

    conn = get_connection()
    import psycopg2.extras
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    conn.close()

    return jsonify([{
        'id': r['id'],
        'tarih': r['kayit_tarihi'].strftime('%d.%m.%Y') if r['kayit_tarihi'] else '',
        'saat': r['kayit_tarihi'].strftime('%H:%M') if r['kayit_tarihi'] else '',
        'musteri_id': r['musteri_id'],
        'musteri_adi': (r['musteri_adi'] or '').strip(),
        'tutar': float(r['odeme_tutari']),
        'kapanan': int(r['kapanan_sayisi']),
    } for r in rows])

@app.route('/api/rapor/urun_bazli')
def api_rapor_urun_bazli():
    baslangic = request.args.get('baslangic', date.today().isoformat())
    bitis = request.args.get('bitis', date.today().isoformat())

    rows = query(
        "SELECT s.sAciklama, s.sKodu, s.sBirimCinsi1, "
        "SUM(d.lCikisMiktar1) AS toplam_miktar, "
        "SUM(d.lCikisTutar) AS toplam_tutar, "
        "COUNT(DISTINCT d.nAlisverisID) AS islem_adedi "
        "FROM tbStokFisiDetayi d "
        "JOIN tbStok s ON d.nStokID = s.nStokID "
        "WHERE CAST(d.dteIslemTarihi AS DATE) >= ? "
        "AND CAST(d.dteIslemTarihi AS DATE) <= ? "
        "AND d.lCikisTutar < 10000000 AND d.nGirisCikis = 3 "
        "GROUP BY s.sAciklama, s.sKodu, s.sBirimCinsi1 "
        "ORDER BY toplam_tutar DESC",
        [baslangic, bitis]
    )

    toplam_ciro = sum(float(r['toplam_tutar']) for r in rows)

    return jsonify({
        'baslangic': baslangic,
        'bitis': bitis,
        'toplam_ciro': toplam_ciro,
        'urun_sayisi': len(rows),
        'urunler': [{
            'ad': (r['sAciklama'] or '').strip(),
            'kod': (r['sKodu'] or '').strip(),
            'birim': (r['sBirimCinsi1'] or 'AD').strip(),
            'miktar': float(r['toplam_miktar']),
            'tutar': float(r['toplam_tutar']),
            'islem': int(r['islem_adedi']),
            'oran': round(float(r['toplam_tutar']) / toplam_ciro * 100, 1) if toplam_ciro > 0 else 0,
        } for r in rows],
    })


# --- API: CANLI SATIS TAKIP ---

@app.route('/canli')
def canli():
    return render_template('canli.html')

@app.route('/api/rapor/canli')
def api_rapor_canli():
    tarih = request.args.get('tarih', date.today().isoformat())

    # Bugunun ozet bilgileri
    ozet = query(
        "SELECT COUNT(*) AS islem_adedi, "
        "ISNULL(SUM(lNetTutar), 0) AS toplam_ciro, "
        "ISNULL(AVG(lNetTutar), 0) AS ort_fis "
        "FROM tbAlisVeris WHERE CAST(dteFaturaTarihi AS DATE) = ? "
        "AND lNetTutar < 10000000",
        [tarih]
    )

    # Eleman bazinda satis ozeti (kasiyer ismiyle)
    eleman_rows = query(
        "SELECT ISNULL(k.sAdi, a.sKasiyerRumuzu) AS eleman_adi, "
        "COUNT(*) AS islem_adedi, SUM(a.lNetTutar) AS toplam_ciro "
        "FROM tbAlisVeris a "
        "LEFT JOIN tbKasiyer k ON RTRIM(a.sKasiyerRumuzu) = RTRIM(k.sKasiyerRumuzu) "
        "WHERE CAST(a.dteFaturaTarihi AS DATE) = ? "
        "AND a.lNetTutar < 10000000 "
        "GROUP BY ISNULL(k.sAdi, a.sKasiyerRumuzu) ORDER BY toplam_ciro DESC",
        [tarih]
    )

    # Son satislar (bugunun, kasiyer ismiyle)
    son_satislar = query(
        "SELECT TOP 100 a.nAlisverisID, a.sFisTipi, a.dteFaturaTarihi, "
        "a.dteKayitTarihi, "
        "a.lFaturaNo, a.sAlisverisYapanAdi, a.sAlisverisYapanSoyadi, "
        "a.lToplamMiktar, a.lNetTutar, "
        "ISNULL(k.sAdi, a.sKasiyerRumuzu) AS eleman_adi, a.sMagaza "
        "FROM tbAlisVeris a "
        "LEFT JOIN tbKasiyer k ON RTRIM(a.sKasiyerRumuzu) = RTRIM(k.sKasiyerRumuzu) "
        "WHERE CAST(a.dteFaturaTarihi AS DATE) = ? "
        "AND a.lNetTutar < 10000000 "
        "ORDER BY a.dteKayitTarihi DESC, a.lFaturaNo DESC",
        [tarih]
    )

    return jsonify({
        'tarih': tarih,
        'ozet': {
            'islem_adedi': int(ozet[0]['islem_adedi']),
            'toplam_ciro': float(ozet[0]['toplam_ciro']),
            'ort_fis': float(ozet[0]['ort_fis']),
        },
        'elemanlar': [{
            'ad': (r['eleman_adi'] or '').strip(),
            'islem_adedi': int(r['islem_adedi']),
            'toplam_ciro': float(r['toplam_ciro']),
        } for r in eleman_rows],
        'satislar': [{
            'id': r['nAlisverisID'].strip(),
            'fis_tipi': (r['sFisTipi'] or '').strip(),
            'saat': r['dteKayitTarihi'].strftime('%H:%M') if r['dteKayitTarihi'] else '',
            'tarih': r['dteFaturaTarihi'].strftime('%d.%m.%Y') if r['dteFaturaTarihi'] else '',
            'fis_no': int(r['lFaturaNo']),
            'musteri': f"{(r['sAlisverisYapanAdi'] or '').strip()} {(r['sAlisverisYapanSoyadi'] or '').strip()}".strip(),
            'miktar': float(r['lToplamMiktar']),
            'tutar': float(r['lNetTutar']),
            'eleman': (r['eleman_adi'] or '').strip(),
            'magaza': (r['sMagaza'] or '').strip(),
        } for r in son_satislar],
    })


# --- EXCEL EXPORT ---

def make_excel(sheets):
    """sheets: [{'baslik': str, 'sutunlar': [str], 'satirlar': [[val,...]]}]"""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    wb = Workbook()
    wb.remove(wb.active)

    header_fill = PatternFill('solid', fgColor='1F4E79')
    header_font = Font(bold=True, color='FFFFFF', size=11)
    thin = Side(style='thin', color='D0D0D0')
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for sh in sheets:
        ws = wb.create_sheet(title=sh['baslik'][:31])
        cols = sh['sutunlar']
        # Baslik satiri
        for ci, col in enumerate(cols, 1):
            cell = ws.cell(row=1, column=ci, value=col)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = border
        ws.row_dimensions[1].height = 20
        # Veri satirlari
        for ri, row in enumerate(sh['satirlar'], 2):
            alt = ri % 2 == 0
            for ci, val in enumerate(row, 1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.border = border
                if alt:
                    cell.fill = PatternFill('solid', fgColor='EEF4FB')
                if isinstance(val, float):
                    cell.number_format = '#,##0.00'
                    cell.alignment = Alignment(horizontal='right')
        # Kolon genislikleri
        for ci, col in enumerate(cols, 1):
            max_len = max([len(str(col))] + [len(str(r[ci-1] or '')) for r in sh['satirlar']], default=10)
            ws.column_dimensions[ws.cell(1, ci).column_letter].width = min(max_len + 3, 40)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf

def excel_response(buf, dosya_adi):
    return Response(
        buf.read(),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{dosya_adi}"'}
    )

@app.route('/api/export/borclu')
def export_borclu():
    rows = query(
        "SELECT m.nMusteriID, m.sAdi, m.sSoyadi, m.sGSM, "
        "COUNT(*) AS veresiye_sayisi, ISNULL(SUM(a.lNetTutar), 0) AS toplam_borc, "
        "MAX(a.dteKayitTarihi) AS son_islem, MIN(a.dteKayitTarihi) AS ilk_borc "
        "FROM tbAlisVeris a JOIN tbMusteri m ON a.nMusteriID = m.nMusteriID "
        "JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "WHERE RTRIM(o.sOdemeSekli) = 'V' AND a.lNetTutar < 10000000 AND a.nMusteriID > 0 "
        "GROUP BY m.nMusteriID, m.sAdi, m.sSoyadi, m.sGSM ORDER BY toplam_borc DESC"
    )
    now = datetime.now()
    satirlar = [[
        (r['sAdi'] or '').strip() + ' ' + (r['sSoyadi'] or '').strip(),
        (r['sGSM'] or '').strip(),
        int(r['veresiye_sayisi']),
        round(float(r['toplam_borc']), 2),
        r['son_islem'].strftime('%d.%m.%Y') if r['son_islem'] else '',
        (now - r['ilk_borc']).days if r['ilk_borc'] else 0,
    ] for r in rows]
    toplam = sum(s[3] for s in satirlar)
    satirlar.append(['TOPLAM', '', '', toplam, '', ''])
    buf = make_excel([{
        'baslik': 'Borclu Musteriler',
        'sutunlar': ['Musteri', 'Telefon', 'Veresiye Islem', 'Toplam Borc (TL)', 'Son Islem', 'Kac Gun'],
        'satirlar': satirlar,
    }])
    tarih = date.today().strftime('%Y%m%d')
    return excel_response(buf, f'borclu_{tarih}.xlsx')

@app.route('/api/export/kasa')
def export_kasa():
    tarih = request.args.get('tarih', date.today().isoformat())
    # Ozet
    ozet_rows = query(
        "SELECT RTRIM(o.sOdemeSekli) AS sekil, COUNT(*) AS islem_adedi, "
        "ISNULL(SUM(a.lNetTutar), 0) AS toplam "
        "FROM tbAlisVeris a JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "WHERE CAST(a.dteFaturaTarihi AS DATE) = ? AND a.lNetTutar < 10000000 "
        "GROUP BY RTRIM(o.sOdemeSekli)", [tarih]
    )
    sekil_ad = {'N': 'Nakit', 'K': 'Kredi Karti', '1': 'Kredi Karti',
                'V': 'Veresiye', 'T': 'Tahsilat', '2': 'Kart(2)', '3': 'Cek(3)'}
    ozet_satirlar = [[sekil_ad.get((r['sekil'] or '').strip(), (r['sekil'] or '-').strip()),
                      int(r['islem_adedi']), round(float(r['toplam']), 2)] for r in ozet_rows]
    # Kasiyere gore
    k_rows = query(
        "SELECT ISNULL(k.sAdi, RTRIM(a.sKasiyerRumuzu)) AS eleman_adi, "
        "RTRIM(o.sOdemeSekli) AS sekil, COUNT(*) AS islem_adedi, "
        "ISNULL(SUM(a.lNetTutar), 0) AS toplam "
        "FROM tbAlisVeris a JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "LEFT JOIN tbKasiyer k ON RTRIM(a.sKasiyerRumuzu) = RTRIM(k.sKasiyerRumuzu) "
        "WHERE CAST(a.dteFaturaTarihi AS DATE) = ? AND a.lNetTutar < 10000000 "
        "GROUP BY ISNULL(k.sAdi, RTRIM(a.sKasiyerRumuzu)), RTRIM(o.sOdemeSekli) "
        "ORDER BY eleman_adi", [tarih]
    )
    k_satirlar = [[(r['eleman_adi'] or 'Bilinmiyor').strip() or 'Bilinmiyor',
                   sekil_ad.get((r['sekil'] or '').strip(), (r['sekil'] or '-').strip()),
                   int(r['islem_adedi']), round(float(r['toplam']), 2)] for r in k_rows]
    buf = make_excel([
        {'baslik': 'Ozet', 'sutunlar': ['Odeme Sekli', 'Islem Adedi', 'Tutar (TL)'], 'satirlar': ozet_satirlar},
        {'baslik': 'Kasiyere Gore', 'sutunlar': ['Kasiyer', 'Odeme Sekli', 'Islem', 'Tutar (TL)'], 'satirlar': k_satirlar},
    ])
    return excel_response(buf, f'kasa_{tarih}.xlsx')

@app.route('/api/export/urun_rapor')
def export_urun_rapor():
    baslangic = request.args.get('baslangic', date.today().isoformat())
    bitis = request.args.get('bitis', date.today().isoformat())
    rows = query(
        "SELECT s.sAciklama, s.sKodu, s.sBirimCinsi1, "
        "SUM(d.lCikisMiktar1) AS toplam_miktar, SUM(d.lCikisTutar) AS toplam_tutar, "
        "COUNT(DISTINCT d.nAlisverisID) AS islem_adedi "
        "FROM tbStokFisiDetayi d JOIN tbStok s ON d.nStokID = s.nStokID "
        "WHERE CAST(d.dteIslemTarihi AS DATE) >= ? AND CAST(d.dteIslemTarihi AS DATE) <= ? "
        "AND d.lCikisTutar < 10000000 AND d.nGirisCikis = 3 "
        "GROUP BY s.sAciklama, s.sKodu, s.sBirimCinsi1 ORDER BY toplam_tutar DESC",
        [baslangic, bitis]
    )
    toplam_ciro = sum(float(r['toplam_tutar']) for r in rows)
    satirlar = [[
        (r['sAciklama'] or '').strip(), (r['sKodu'] or '').strip(),
        (r['sBirimCinsi1'] or '').strip(),
        round(float(r['toplam_miktar']), 3), round(float(r['toplam_tutar']), 2),
        int(r['islem_adedi']),
        round(float(r['toplam_tutar']) / toplam_ciro * 100, 1) if toplam_ciro > 0 else 0,
    ] for r in rows]
    buf = make_excel([{
        'baslik': 'Urun Raporu',
        'sutunlar': ['Urun Adi', 'Kodu', 'Birim', 'Miktar', 'Tutar (TL)', 'Islem', 'Oran (%)'],
        'satirlar': satirlar,
    }])
    return excel_response(buf, f'urun_raporu_{baslangic}_{bitis}.xlsx')

@app.route('/api/export/veresiye_gecmis')
def export_veresiye_gecmis():
    if DB_MODE != 'postgres':
        return jsonify({'error': 'Sadece PostgreSQL'}), 400
    bas = request.args.get('baslangic', date.today().isoformat())
    bit = request.args.get('bitis', date.today().isoformat())
    conn = get_connection()
    import psycopg2.extras
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(
        "SELECT kayit_tarihi, musteri_adi, odeme_tutari, kapanan_sayisi "
        "FROM veresiye_odeme_log "
        "WHERE kayit_tarihi::date >= %s AND kayit_tarihi::date <= %s "
        "ORDER BY kayit_tarihi DESC",
        [bas, bit]
    )
    rows = cursor.fetchall()
    conn.close()
    satirlar = [[
        r['kayit_tarihi'].strftime('%d.%m.%Y') if r['kayit_tarihi'] else '',
        r['kayit_tarihi'].strftime('%H:%M') if r['kayit_tarihi'] else '',
        (r['musteri_adi'] or '').strip(),
        round(float(r['odeme_tutari']), 2),
        int(r['kapanan_sayisi']),
    ] for r in rows]
    buf = make_excel([{
        'baslik': 'Veresiye Odeme Gecmisi',
        'sutunlar': ['Tarih', 'Saat', 'Musteri', 'Tutar (TL)', 'Kapanan Kayit'],
        'satirlar': satirlar,
    }])
    return excel_response(buf, f'veresiye_gecmis_{bas}_{bit}.xlsx')

@app.route('/api/export/gun_sonu')
def export_gun_sonu():
    tarih = request.args.get('tarih', date.today().isoformat())

    # Kasa ozet
    ozet_rows = query(
        "SELECT RTRIM(o.sOdemeSekli) AS sekil, "
        "COUNT(*) AS islem_adedi, ISNULL(SUM(a.lNetTutar), 0) AS toplam "
        "FROM tbAlisVeris a "
        "JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "WHERE CAST(a.dteFaturaTarihi AS DATE) = ? "
        "AND a.lNetTutar < 10000000 "
        "GROUP BY RTRIM(o.sOdemeSekli)", [tarih]
    )
    sekil_ad = {'N': 'Nakit', 'K': 'Kredi Karti', '1': 'Kredi Karti', 'V': 'Veresiye', 'T': 'Odendi'}
    ozet_satirlar = [[sekil_ad.get((r['sekil'] or '').strip(), r['sekil']),
                      int(r['islem_adedi']), round(float(r['toplam']), 2)]
                     for r in ozet_rows]

    # Kasiyere gore
    kasiyer_rows = query(
        "SELECT ISNULL(k.sAdi, RTRIM(a.sKasiyerRumuzu)) AS eleman_adi, "
        "RTRIM(o.sOdemeSekli) AS sekil, "
        "COUNT(*) AS islem_adedi, ISNULL(SUM(a.lNetTutar), 0) AS toplam "
        "FROM tbAlisVeris a "
        "JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "LEFT JOIN tbKasiyer k ON RTRIM(a.sKasiyerRumuzu) = RTRIM(k.sKasiyerRumuzu) "
        "WHERE CAST(a.dteFaturaTarihi AS DATE) = ? "
        "AND a.lNetTutar < 10000000 "
        "GROUP BY ISNULL(k.sAdi, RTRIM(a.sKasiyerRumuzu)), RTRIM(o.sOdemeSekli) "
        "ORDER BY eleman_adi", [tarih]
    )
    kasiyerler = {}
    for r in kasiyer_rows:
        ad = (r['eleman_adi'] or '').strip() or 'Bilinmiyor'
        s = (r['sekil'] or '').strip().upper()
        if ad not in kasiyerler:
            kasiyerler[ad] = {'ad': ad, 'nakit': 0, 'kart': 0, 'veresiye': 0, 'islem': 0}
        kasiyerler[ad]['islem'] += int(r['islem_adedi'])
        if s == 'N': kasiyerler[ad]['nakit'] += float(r['toplam'])
        elif s in ('K', '1'): kasiyerler[ad]['kart'] += float(r['toplam'])
        elif s in ('V', 'T'): kasiyerler[ad]['veresiye'] += float(r['toplam'])
    for k in kasiyerler.values():
        k['toplam'] = k['nakit'] + k['kart'] + k['veresiye']
    kas_satirlar = [[k['ad'], round(k['nakit'], 2), round(k['kart'], 2),
                     round(k['veresiye'], 2), round(k['toplam'], 2), k['islem']]
                    for k in sorted(kasiyerler.values(), key=lambda x: -x['toplam'])]

    # Urun bazli (top 20)
    urun_rows = query(
        "SELECT s.sAciklama, s.sBirimCinsi1, "
        "SUM(d.lCikisMiktar1) AS toplam_miktar, SUM(d.lCikisTutar) AS toplam_tutar "
        "FROM tbStokFisiDetayi d "
        "JOIN tbStok s ON d.nStokID = s.nStokID "
        "WHERE CAST(d.dteIslemTarihi AS DATE) = ? "
        "AND d.lCikisTutar < 10000000 AND d.nGirisCikis = 3 "
        "GROUP BY s.sAciklama, s.sBirimCinsi1 "
        "ORDER BY toplam_tutar DESC",
        [tarih]
    )
    toplam_ciro = sum(float(r['toplam_tutar']) for r in urun_rows)
    urun_satirlar = [[(r['sAciklama'] or '').strip(),
                      round(float(r['toplam_miktar']), 3),
                      (r['sBirimCinsi1'] or 'AD').strip(),
                      round(float(r['toplam_tutar']), 2),
                      round(float(r['toplam_tutar']) / toplam_ciro * 100, 1) if toplam_ciro > 0 else 0]
                     for r in urun_rows]

    buf = make_excel([
        {'baslik': f'Gun Sonu - {tarih}',
         'sutunlar': ['Odeme Tipi', 'Islem Sayisi', 'Tutar (TL)'],
         'satirlar': ozet_satirlar},
        {'baslik': 'Kasiyere Gore',
         'sutunlar': ['Kasiyer', 'Nakit', 'Kart', 'Veresiye', 'Toplam', 'Islem'],
         'satirlar': kas_satirlar},
        {'baslik': 'Urun Detayi',
         'sutunlar': ['Urun', 'Miktar', 'Birim', 'Tutar (TL)', 'Oran (%)'],
         'satirlar': urun_satirlar},
    ])
    return excel_response(buf, f'gun_sonu_{tarih}.xlsx')

@app.route('/api/export/musteri/<int:musteri_id>')
def export_musteri(musteri_id):
    m = query("SELECT sAdi, sSoyadi FROM tbMusteri WHERE nMusteriID = ?", [musteri_id])
    ad = ((m[0]['sAdi'] or '') + ' ' + (m[0]['sSoyadi'] or '')).strip() if m else str(musteri_id)
    rows = query(
        "SELECT a.lFaturaNo, a.dteFaturaTarihi, a.dteKayitTarihi, "
        "a.lNetTutar, a.lToplamMiktar, RTRIM(ISNULL(o.sOdemeSekli,'')) AS odeme_sekli "
        "FROM tbAlisVeris a "
        "LEFT JOIN tbOdeme o ON RTRIM(a.nAlisverisID) = RTRIM(o.nAlisverisID) "
        "WHERE a.nMusteriID = ? AND a.lNetTutar < 10000000 "
        "ORDER BY a.dteKayitTarihi DESC", [musteri_id]
    )
    sekil_ad = {'N': 'Nakit', 'K': 'Kredi Karti', '1': 'Kredi Karti', 'V': 'Veresiye', 'T': 'Odendi'}
    satirlar = [[
        a['dteFaturaTarihi'].strftime('%d.%m.%Y') if a['dteFaturaTarihi'] else '',
        a['dteKayitTarihi'].strftime('%H:%M') if a['dteKayitTarihi'] else '',
        int(a['lFaturaNo']),
        sekil_ad.get((a['odeme_sekli'] or '').strip(), a['odeme_sekli']),
        round(float(a['lNetTutar']), 2),
        round(float(a['lToplamMiktar']), 3),
    ] for a in rows]
    buf = make_excel([{
        'baslik': 'Alisveris Gecmisi',
        'sutunlar': ['Tarih', 'Saat', 'Fis No', 'Odeme', 'Tutar (TL)', 'Miktar'],
        'satirlar': satirlar,
    }])
    temiz_ad = ad.replace(' ', '_')[:20]
    return excel_response(buf, f'musteri_{temiz_ad}.xlsx')


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
CREATE TABLE tbkasiyer (
    skasiyerrumuzu VARCHAR(4) DEFAULT '', sadi VARCHAR(60) DEFAULT '',
    ssoyadi VARCHAR(60) DEFAULT '', ssifresi VARCHAR(20) DEFAULT '',
    sdepo VARCHAR(4) DEFAULT '', biptalyapabilirmi BOOLEAN DEFAULT FALSE,
    bvadefarksizodemealirmi BOOLEAN DEFAULT FALSE, nmaxiskontoyuzdesi NUMERIC DEFAULT 0
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
CREATE TABLE IF NOT EXISTS tbstokfisimaster (
    nstokfisiid INTEGER PRIMARY KEY,
    sfistipi VARCHAR(3) DEFAULT '', dtefistarihi TIMESTAMP, ngiriscikis NUMERIC DEFAULT 0,
    lfisno NUMERIC DEFAULT 0, nfirmaid INTEGER DEFAULT 0, sdepo VARCHAR(4) DEFAULT '',
    dtevalortarihi TIMESTAMP, bpesinmi BOOLEAN DEFAULT FALSE, blistelendimi BOOLEAN DEFAULT FALSE,
    bhizmetfaturasimi BOOLEAN DEFAULT FALSE,
    ltoplammiktar NUMERIC DEFAULT 0, lmalbedeli NUMERIC DEFAULT 0, lmaliskontotutari NUMERIC DEFAULT 0,
    ndipiskontoyuzdesi1 NUMERIC DEFAULT 0, ldipiskontotutari1 NUMERIC DEFAULT 0,
    ndipiskontoyuzdesi2 NUMERIC DEFAULT 0, ldipiskontotutari2 NUMERIC DEFAULT 0,
    ldipiskontotutari3 NUMERIC DEFAULT 0,
    lekmaliyet1 NUMERIC DEFAULT 0, lekmaliyet2 NUMERIC DEFAULT 0, lekmaliyet3 NUMERIC DEFAULT 0,
    nkdvorani1 NUMERIC DEFAULT 0, lkdvmatrahi1 NUMERIC DEFAULT 0, lkdv1 NUMERIC DEFAULT 0,
    nkdvorani2 NUMERIC DEFAULT 0, lkdvmatrahi2 NUMERIC DEFAULT 0, lkdv2 NUMERIC DEFAULT 0,
    nkdvorani3 NUMERIC DEFAULT 0, lkdvmatrahi3 NUMERIC DEFAULT 0, lkdv3 NUMERIC DEFAULT 0,
    nkdvorani4 NUMERIC DEFAULT 0, lkdvmatrahi4 NUMERIC DEFAULT 0, lkdv4 NUMERIC DEFAULT 0,
    nkdvorani5 NUMERIC DEFAULT 0, lkdvmatrahi5 NUMERIC DEFAULT 0, lkdv5 NUMERIC DEFAULT 0,
    lnettutar NUMERIC DEFAULT 0, ntevkifatkdvorani NUMERIC DEFAULT 0,
    ltevkifatkdvmatrahi NUMERIC DEFAULT 0, ltevkifatkdv NUMERIC DEFAULT 0,
    sharekettipi VARCHAR(20) DEFAULT '', bmuhasebeyeislendimi BOOLEAN DEFAULT FALSE,
    bfistamamlandimi BOOLEAN DEFAULT FALSE, ltransferFisiid NUMERIC DEFAULT 0,
    stransferdepo VARCHAR(4) DEFAULT '', bfaturayadonustumu BOOLEAN DEFAULT FALSE,
    skullaniciadi VARCHAR(60) DEFAULT '', dtekayittarihi TIMESTAMP, syaziile VARCHAR(60) DEFAULT '',
    notvorani1 NUMERIC DEFAULT 0, lotvmatrahi1 NUMERIC DEFAULT 0, lotv1 NUMERIC DEFAULT 0,
    notvorani2 NUMERIC DEFAULT 0, lotvmatrahi2 NUMERIC DEFAULT 0, lotv2 NUMERIC DEFAULT 0,
    bkilitli BOOLEAN DEFAULT FALSE, befatura BOOLEAN DEFAULT FALSE,
    sefaturatipi VARCHAR(20) DEFAULT '', sefaturaguid VARCHAR(40) DEFAULT '',
    nefaturadurum NUMERIC DEFAULT 0
);
"""

CREATE_TBFIRMA_SQL = """
CREATE TABLE IF NOT EXISTS tbfirma (
    nfirmaid INTEGER PRIMARY KEY,
    skodu VARCHAR(20) DEFAULT '',
    saciklama VARCHAR(60) DEFAULT ''
)
"""

CREATE_TBSTOKFISIMASTER_SQL = """
CREATE TABLE IF NOT EXISTS tbstokfisimaster (
    nstokfisiid INTEGER PRIMARY KEY,
    sfistipi VARCHAR(3) DEFAULT '', dtefistarihi TIMESTAMP, ngiriscikis NUMERIC DEFAULT 0,
    lfisno NUMERIC DEFAULT 0, nfirmaid INTEGER DEFAULT 0, sdepo VARCHAR(4) DEFAULT '',
    dtevalortarihi TIMESTAMP, bpesinmi BOOLEAN DEFAULT FALSE, blistelendimi BOOLEAN DEFAULT FALSE,
    bhizmetfaturasimi BOOLEAN DEFAULT FALSE,
    ltoplammiktar NUMERIC DEFAULT 0, lmalbedeli NUMERIC DEFAULT 0, lmaliskontotutari NUMERIC DEFAULT 0,
    ndipiskontoyuzdesi1 NUMERIC DEFAULT 0, ldipiskontotutari1 NUMERIC DEFAULT 0,
    ndipiskontoyuzdesi2 NUMERIC DEFAULT 0, ldipiskontotutari2 NUMERIC DEFAULT 0,
    ldipiskontotutari3 NUMERIC DEFAULT 0,
    lekmaliyet1 NUMERIC DEFAULT 0, lekmaliyet2 NUMERIC DEFAULT 0, lekmaliyet3 NUMERIC DEFAULT 0,
    nkdvorani1 NUMERIC DEFAULT 0, lkdvmatrahi1 NUMERIC DEFAULT 0, lkdv1 NUMERIC DEFAULT 0,
    nkdvorani2 NUMERIC DEFAULT 0, lkdvmatrahi2 NUMERIC DEFAULT 0, lkdv2 NUMERIC DEFAULT 0,
    nkdvorani3 NUMERIC DEFAULT 0, lkdvmatrahi3 NUMERIC DEFAULT 0, lkdv3 NUMERIC DEFAULT 0,
    nkdvorani4 NUMERIC DEFAULT 0, lkdvmatrahi4 NUMERIC DEFAULT 0, lkdv4 NUMERIC DEFAULT 0,
    nkdvorani5 NUMERIC DEFAULT 0, lkdvmatrahi5 NUMERIC DEFAULT 0, lkdv5 NUMERIC DEFAULT 0,
    lnettutar NUMERIC DEFAULT 0, ntevkifatkdvorani NUMERIC DEFAULT 0,
    ltevkifatkdvmatrahi NUMERIC DEFAULT 0, ltevkifatkdv NUMERIC DEFAULT 0,
    sharekettipi VARCHAR(20) DEFAULT '', bmuhasebeyeislendimi BOOLEAN DEFAULT FALSE,
    bfistamamlandimi BOOLEAN DEFAULT FALSE, ltransferfisiid NUMERIC DEFAULT 0,
    stransferdepo VARCHAR(4) DEFAULT '', bfaturayadonustumu BOOLEAN DEFAULT FALSE,
    skullaniciadi VARCHAR(60) DEFAULT '', dtekayittarihi TIMESTAMP, syaziile VARCHAR(60) DEFAULT '',
    notvorani1 NUMERIC DEFAULT 0, lotvmatrahi1 NUMERIC DEFAULT 0, lotv1 NUMERIC DEFAULT 0,
    notvorani2 NUMERIC DEFAULT 0, lotvmatrahi2 NUMERIC DEFAULT 0, lotv2 NUMERIC DEFAULT 0,
    bkilitli BOOLEAN DEFAULT FALSE, befatura BOOLEAN DEFAULT FALSE,
    sefaturatipi VARCHAR(20) DEFAULT '', sefaturaguid VARCHAR(40) DEFAULT '',
    nefaturadurum NUMERIC DEFAULT 0
)
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
-- Duplicate kayitlari temizle, sonra unique constraint ekle
DELETE FROM tbstokfisidetayi a USING tbstokfisidetayi b WHERE a.ctid < b.ctid AND a.nislemid = b.nislemid;
DELETE FROM tbalisveris a USING tbalisveris b WHERE a.ctid < b.ctid AND a.nalisverisid = b.nalisverisid;
DELETE FROM tbodeme a USING tbodeme b WHERE a.ctid < b.ctid AND a.nodemeid = b.nodemeid;
DELETE FROM tbmusteri a USING tbmusteri b WHERE a.ctid < b.ctid AND a.nmusteriid = b.nmusteriid;
DO $$ BEGIN
  BEGIN ALTER TABLE tbstokfisidetayi ADD CONSTRAINT tbstokfisidetayi_nislemid_key UNIQUE (nislemid); EXCEPTION WHEN others THEN NULL; END;
  BEGIN ALTER TABLE tbalisveris ADD CONSTRAINT tbalisveris_nalisverisid_key UNIQUE (nalisverisid); EXCEPTION WHEN others THEN NULL; END;
  BEGIN ALTER TABLE tbodeme ADD CONSTRAINT tbodeme_nodemeid_key UNIQUE (nodemeid); EXCEPTION WHEN others THEN NULL; END;
  BEGIN ALTER TABLE tbmusteri ADD CONSTRAINT tbmusteri_nmusteriid_key UNIQUE (nmusteriid); EXCEPTION WHEN others THEN NULL; END;
  BEGIN ALTER TABLE tbstokfisimaster ADD CONSTRAINT tbstokfisimaster_nstokfisiid_key UNIQUE (nstokfisiid); EXCEPTION WHEN others THEN NULL; END;
END $$;
CREATE INDEX IF NOT EXISTS idx_sfm_tarih ON tbstokfisimaster (dtefistarihi);
CREATE INDEX IF NOT EXISTS idx_sfm_fistipi ON tbstokfisimaster (sfistipi);
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

    table_lower = table.lower()
    col_list = [c.lower() for c in cols]
    col_names = ', '.join(col_list)
    placeholders = ', '.join(['%s'] * len(cols))

    # Primary key mapping for UPSERT
    pk_map = {
        'tbalisveris': 'nalisverisid',
        'tbstok': 'nstokid',
        'tbstokbarkodu': None,
        'tbstokfiyati': None,
        'tbstoksinifi': None,
        'tbkasiyer': None,
        'tbstokfisidetayi': 'nislemid',
        'tbodeme': 'nodemeid',
        'tbmusteri': 'nmusteriid',
        'tbstokfisimaster': 'nstokfisiid',
        'tbfirma': 'nfirmaid',
    }
    pk = pk_map.get(table_lower)

    if pk and pk in col_list:
        # UPSERT: INSERT ON CONFLICT UPDATE
        update_cols = [c for c in col_list if c != pk]
        update_set = ', '.join([f'{c} = EXCLUDED.{c}' for c in update_cols])
        sql = (f'INSERT INTO {table_lower} ({col_names}) VALUES ({placeholders}) '
               f'ON CONFLICT ({pk}) DO UPDATE SET {update_set}')
    else:
        sql = f'INSERT INTO {table_lower} ({col_names}) VALUES ({placeholders})'

    conn = get_connection()
    cursor = conn.cursor()
    count = 0
    errors = []
    for i, row in enumerate(rows):
        try:
            cursor.execute(sql, row)
            count += 1
        except Exception as e:
            conn.rollback()
            if len(errors) < 3:
                errors.append(f"row {i}: {str(e)[:200]}")
            continue
    conn.commit()
    conn.close()
    result = {'ok': True, 'inserted': count, 'total': len(rows)}
    if errors:
        result['errors'] = errors
    return jsonify(result)

@app.route('/api/migrate/index', methods=['POST'])
def api_migrate_index():
    if request.json.get('secret') != MIGRATE_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(CREATE_TBFIRMA_SQL)
    cursor.execute("DROP TABLE IF EXISTS tbstokfisimaster")
    cursor.execute(CREATE_TBSTOKFISIMASTER_SQL)
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
        'tbStokFisiMaster': 'nstokfisiid',
        'tbFirma': 'nfirmaid',
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


def keep_alive():
    """Render free tier uyku modunu engelle - her 5 dakikada self-ping."""
    import time
    url = os.environ.get('RENDER_EXTERNAL_URL', '')
    if not url:
        return
    while True:
        time.sleep(300)
        try:
            req.get(f"{url}/healthz", timeout=10)
        except:
            pass

if os.environ.get('RENDER'):
    t = threading.Thread(target=keep_alive, daemon=True)
    t.start()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
