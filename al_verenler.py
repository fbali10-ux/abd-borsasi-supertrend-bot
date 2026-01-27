import os
import time
import hashlib
from datetime import datetime, timezone, timedelta

import requests
import pandas as pd
import numpy as np
import yfinance as yf
from curl_cffi import requests as cffi_requests


# =======================
# 1) SİMGE LİSTESİ (SENİN LİSTE)
# =======================
SYMBOLS = [
"NVDA",
"GOOGL",
"GOOG",
"AAPL",
"MSFT",
"AMZN",
"TSM",
"META",
"AVGO",
"TSLA",
"BRK-A",
"BRK-B",
"JPM",
"LLY",
"WMT",
"V",
"XOM",
"ASML",
"JNJ",
"ORCL",
"BAC",
"MA",
"MU",
"COST",
"BABA",
"AMD",
"PLTR",
"ABBV",
"HD",
"NFLX",
"GS",
"PG",
"CVX",
"JPM-PD",
"JPM-PC",
"UNH",
"MS",
"KO",
"BAC-PK",
"GE",
"BAC-PL",
"CSCO",
"CAT",
"TM",
"AZN",
"HSBC",
"C",
"NVS",
"NVO",
"LRCX",
"SAP",
"IBM",
"WFC",
"BML-PG",
"BML-PH",
"PM",
"MRK",
"BAC-PE",
"BML-PL",
"RTX",
"AMAT",
"BAC-PB",
"BML-PJ",
"AXP",
"RY",
"TMO",
"MCD",
"CRM",
"LIN",
"INTC",
"TMUS",
"SHEL",
"MUFG",
"KLAC",
"WFC-PY",
"PEP",
"DIS",
"BA",
"WFC-PL",
"APH",
"ISRG",
"ABT",
"AMGN",
"SAN",
"SCHW",
"BX",
"GEV",
"APP",
"ANET",
"RIO",
"TXN",
"NEE",
"SHOP",
"TD",
"BHP",
"ACN",
"BLK",
"UBER",
"GILD",
"DHR",
"T",
"VZ",
"TJX",
"BKNG",
"QCOM",
"HDB",
"SPGI",
"INTU",
"LOW",
"SCCO",
"UBS",
"PDD",
"TTE",
"HON",
"ADI",
"PFE",
"UL",
"BBVA",
"NOW",
"DE",
"COF",
"BSX",
"NEM",
"SONY",
"UNP",
"SYK",
"LMT",
"SMFG",
"BUD",
"ETN",
"MDT",
"PANW",
"IBKR",
"BTI",
"ADBE",
"WELL",
"WFC-PC",
"COP",
"VRTX",
"PGR",
"ARM",
"CB",
"PH",
"CRWD",
"PLD",
"CMCSA",
"SNY",
"MELI",
"BMY",
"HCA",
"KKR",
"SBUX",
"AEM",
"CVS",
"MO",
"BN",
"SPOT",
"MFG",
"ADP",
"ENB",
"IBN",
"CEG",
"MCK",
"CVNA",
"LYG",
"CME",
"GSK",
"ICE",
"GD",
"SO",
"SNPS",
"HOOD",
"NKE",
"NOC",
"BP",
"MCO",
"WM",
"BNS",
"ITUB",
"PBR",
"DUK",
"BCS",
"UPS",
"MRSH",
"DASH",
"PBR-A",
"PNC",
"NU",
"BMO",
"FCX",
"CDNS",
"TT",
"B",
"SHW",
"USB",
"HWM",
"MAR",
"NTES",
"ELV",
"ORLY",
"MMM",
"ING",
"MS-PK",
"AMT",
"EMR",
"BK",
"WDC",
"BAM",
"MS-PI",
"NGG",
"CRH",
"ABNB",
"GLW",
"TDG",
"REGN",
"DB",
"MS-PF",
"ECL",
"RCL",
"MS-PE",
"MNST",
"EQIX",
"CMI",
"WMB",
"USB-PH",
"STX",
"CTAS",
"DELL",
"APO",
"MDLZ",
"GM",
"ITW",
"GS-PA",
"CNQ",
"INFY",
"CI",
"SE",
"SLB",
"USB-PP",
"AON",
"SNOW",
"GS-PD",
"FDX",
"RELX",
"EPD",
"NWG",
"MRVL",
"JCI",
"MS-PA",
"PWR",
"WBD",
"SPG",
"HLT",
"CSX",
"VRT",
"COR",
"SNDK",
"CL",
"RSG",
"WPM",
"VALE",
"MSI",
"CP",
"TEL",
"NET",
"LHX",
"AJG",
"KMI",
"NSC",
"TFC",
"PCAR",
"EQNR",
"FTNT",
"AEP",
"AZO",
"TRV",
"CNI",
"MFC",
"AMX",
"SU",
"DUK-PA",
"ET",
"ROST",
"RACE",
"RKT",
"E",
"CTA-PB",
"EOG",
"TRP",
"URI",
"APD",
"AFL",
"NXPI",
"BDX",
"ADSK",
"COIN",
"VLO",
"NDAQ",
"PSX",
"SRE",
"DLR",
"IDXX",
"MPLX",
"TRI",
"BKR",
"O",
"TAK",
"ZTS",
"AU",
"BIDU",
"PYPL",
"VST",
"F",
"CMG",
"CCJ",
"MPC",
"RBLX",
"IMO",
"ALL",
"ARGX",
"D",
"MET",
"MPWR",
"EA",
"SCHW-PD",
"FERG",
"WDAY",
"AME",
"BSBR",
"CBRE",
"GWW",
"FAST",
"FNV",
"CAH",
"DEO",
"EW",
"CTVA",
"GFI",
"ARES",
"CRWV",
"PSA",
"CARR",
"OKE",
"FER",
"AXON",
"DDOG",
"ALNY",
"ROK",
"TGT",
"AMP",
"CTA-PA",
"HEI",
"MSTR",
"KGC",
"HLN",
"MSCI",
"TTWO",
"LNG",
"SPG-PJ",
"EXC",
"XEL",
"FANG",
"DAL",
"WCN",
"ROP",
"ABEV",
"JD",
"DHI",
"OXY",
"ASX",
"EBAY",
"MET-PE",
"RKLB",
"YUM",
"ETR",
"KR",
"BBD",
"MET-PA",
"EL",
"CTSH",
"TCOM",
"LVS",
"TRGP",
"AIG",
"CCEP",
"CM",
"MT",
"IQV",
"NUE",
"PUK",
"RDDT",
"MCHP",
"CPRT",
"XYZ",
"HEI-A",
"ALC",
"GRMN",
"FIX",
"VMC",
"WAB",
"PEG",
"MLM",
"NOK",
"HMC",
"HSY",
"ASTS",
"A",
"BBDO",
"PRU",
"PSA-PH",
"PAYX",
"CCI",
"ED",
"CUK",
"KDP",
"CCL",
"ONC",
"MDLN",
"RMD",
"FICO",
"FMX",
"TEVA",
"TER",
"VEEV",
"KEYS",
"GEHC",
"ODFL",
"FISV",
"HIG",
"TEAM",
"ERIC",
"SYY",
"RYAAY",
"VTR",
"CPNG",
"CVE",
"WEC",
"STT",
"OTIS",
"CLS",
"SYM",
"SLF",
"EQT",
"APO-PA",
"ACGL",
"XYL",
"UAL",
"IR",
"SATS",
"LYV",
"ZS",
"NTR",
"KB",
"INSM",
"KVUE",
"NTRA",
"KMB",
"RJF",
"MTB",
"IX",
"MDB",
"PCG",
"FITB",
"EXPE",
"VOD",
"CHT",
"UI",
"DG",
"WDS",
"BE",
"ESLT",
"PSA-PK",
"ALL-PH",
"CIEN",
"SOFI",
"ADM",
"ALL-PB",
"FOXA",
"HUM",
"EME",
"WTW",
"COHR",
"EXR",
"VIK",
"FIS",
"FOX",
"VRSK",
"QSR",
"FLUT",
"VICI",
"ROL",
"AMRZ",
"ULTA",
"FTAI",
"BNTX",
"MTD",
"TSCO",
"HAL",
"NRG",
"SYF",
"DXCM",
"LPLA",
"ZM",
"UMC",
"TDY",
"HPE",
"TME",
"DOV",
"NTRS",
"CBOE",
"DTE",
"STZ",
"STLA",
"AEE",
"KHC",
"SHG",
"CSGP",
"PHG",
"IRM",
"BAP",
"ALAB",
"CQP",
"PAAS",
"WIT",
"LEN",
"HBAN",
"ATO",
"EC",
"BRO",
"FE",
"PPL",
"KEP",
"CFG",
"TECK",
"EXE",
"FTS",
"NMR",
"CHTR",
"LEN-B",
"EFX",
"ES",
"TPR",
"FSLR",
"STE",
"HUBB",
"JBL",
"CNP",
"MKL",
"AER",
"DLTR",
"AWK",
"STLD",
"OMC",
"PPG",
"STM",
"WRB",
"BIIB",
"AVB",
"VLTO",
"ON",
"DLR-PK",
"CINF",
"FCNCA",
"PHM",
"GFS",
"DVN",
"RGLD",
"CW",
"EQR",
"WSM",
"BR",
"RF",
"LDOS",
"PSTG",
"SQM",
"FLEX",
"GIS",
"AXIA-P",
"EIX",
"RPRX",
"PBA",
"LITE",
"TPL",
"AXIA-PC",
"ILMN",
"VRSN",
"KEY",
"BCE",
"TPG",
"TROW",
"WAT",
"TW",
"CRDO",
"NBIS",
"VG",
"AXIA",
"CPAY",
"CASY",
"LULU",
"DRI",
"OWL",
"CNC",
"IP",
"FUTU",
"AFRM",
"SW",
"DLR-PJ",
"TLK",
"CYBR",
"TSN",
"FTI",
"FWONK",
"CHD",
"VIV",
"ALB",
"BCH",
"PSLV",
"FWONA",
"KOF",
"LH",
"TS",
"CG",
"LUV",
"BG",
"RL",
"RBA",
"CMS",
"CIB",
"EXPD",
"TU",
"UTHR",
"L",
"NVR",
"GPN",
"CHRW",
"AS",
"BEKE",
"SSNC",
"CTRA",
"NI",
"GMAB",
"PFG",
"AMCR",
"TWLO",
"IHG",
"DGX",
"Q",
"INCY",
"HL",
"DOW",
"SBAC",
"PKG",
"WWD",
"RCI",
"CHKP",
"PTC",
"LTM",
"NTAP",
"TOST",
"JBHT",
"EXAS",
"SGI",
"GIB",
"GPC",
"MTZ",
"RIVN",
"SNA",
"WY",
"PODD",
"TYL",
"IFF",
"RVMD",
"PKX",
"BWXT",
"KTOS",
"MRNA",
"HIG-PG",
"GRAB",
"SBS",
"IOT",
"FTV",
"DD",
"CX",
"BURL",
"PHYS",
"U",
"SMCI",
"DKS",
"APG",
"BEP",
"HPQ",
"STT-PG",
"USFD",
"CRCL",
"LII",
"FITBI",
"XPEV",
"IT",
"PSNYW",
"AGI",
"NVT",
"ALLY",
"KEY-PK",
"PNR",
"EVRG",
"PINS",
"ENTG",
"XPO",
"ESS",
"SN",
"CRS",
"WST",
"HUBS",
"ZBH",
"NWS",
"LNT",
"JBS",
"YUMC",
"IREN",
"RS",
"LI",
"ZG",
"BSAC",
"ATI",
"ZTO",
"FN",
"TRMB",
"MEDP",
"JLL",
"QXO",
"TXT",
"Z",
"HOLX",
"THC",
"APTV",
"TKO",
"CDW",
"RTO",
"WES",
"TRU",
"TTD",
"INVH",
"MTSI",
"LYB",
"MKC",
"CDE",
"NLY",
"BIP",
"NXT",
"J",
"MKC-V",
"HII",
"MAA",
"OKTA",
"COO",
"SUI",
"TLN",
"GFL",
"RBC",
"WMG",
"KSPI",
"ITT",
"ROKU",
"ROIV",
"GEN",
"NWSA",
"WSO",
"EWBC",
"BALL",
"FFIV",
"YPF",
"H",
"HTHT",
"IONQ",
"ONON",
"CRBG",
"WSO-B",
"DKNG",
"VTRS",
"WF",
"AA",
"NDSN",
"AVAV",
"MGA",
"KEY-PI",
"WPC",
"DECK",
"EMA",
"CSL",
"BBIO",
"GH",
"FNF",
"MLI",
"CF",
"KEY-PJ",
"HMY",
"PFGC",
"IEX",
"ULS",
"MKSI",
"GDDY",
"ERIE",
"ARCC",
"ICLR",
"SNN",
"AVY",
"FIG",
"RGC",
"PNFP",
"KRMN",
"GGG",
"ALLE",
"MAS",
"RF-PC",
"W",
"TSEM",
"ASND",
"LECO",
"PAC",
"CACI",
"AKAM",
"JHX",
"KIM",
"PEN",
"EVR",
"CELH",
"DPZ",
"BBY",
"EMBJ",
"CLH",
"WCC",
"UDR",
"SBSW",
"TOL",
"LOGI",
"EQH",
"RPM",
"CLX",
"GWRE",
"EG",
"SOLV",
"NVMI",
"BILI",
"RBRK",
"CNH",
"HRL",
"FIGR",
"PAA",
"NLY-PG",
"BLD",
"NLY-PF",
"AMH",
"NBIX",
"OHI",
"BEN",
"RVTY",
"JKHY",
"RYAN",
"BLDR",
"SF",
"RGA",
"IONS",
"PSKY",
"UHS",
"REG",
"HST",
"CHWY",
"LAMR",
"UNM",
"SNAP",
"JEF",
"RNR",
"FMS",
"VNOM",
"BNT",
"BJ",
"OKLO",
"HLI",
"BF-A",
"GLPI",
"EQX",
"IVZ",
"ELS",
"ACM",
"CNA",
"BF-B",
"SWK",
"AGNC",
"BMNR",
"GLXY",
"AG",
"GMED",
"HAS",
"SNX",
"AMKR",
"IAG",
"DT",
"ACGLO",
"ZBRA",
"DTM",
"TXRH",
"DOC",
"SMMT",
"RMBS",
"EPAM",
"ELAN",
"GIL",
"ALGN",
"CCK",
"JOBY",
"SUZ",
"CR",
"CMA",
"TEM",
"WMS",
"FHN",
"WYNN",
"NYT",
"AEG",
"AIZ",
"MAA-PI",
"EXEL",
"NTNX",
"BSY",
"NIO",
"DCI",
"RDY",
"DOCU",
"CPT",
"BXP",
"LSCC",
"STN",
"CEF",
"MDGL",
"BAH",
"SCI",
"MICC",
"CNM",
"MP",
"DY",
"GL",
"TIMB",
"QGEN",
"WTRG",
"PNW",
"DAY",
"STRL",
"SJM",
"PR",
"SARO",
"RNA",
"CRL",
"AR",
"UHAL",
"BMRN",
"MOH",
"SUN",
"CFLT",
"DRS",
"WLK",
"GME",
"SPXC",
"MANH",
"FDS",
"AFG",
"CART",
"AIT",
"PAG",
"SEIC",
"TECH",
"CAE",
"OVV",
"WBS",
"YMM",
"AES",
"FIVE",
"XP",
"BWA",
"ENSG",
"ASR",
"ONTO",
"OC",
"TIGO",
"PCOR",
"KLAR",
"ARMK",
"BPYPP",
"FLS",
"RRX",
"APLD",
"JAZZ",
"BAX",
"PPC",
"SSB",
"HBM",
"CHYM",
"VNO-PL",
"UHAL-B",
"SAIL",
"SANM",
"WTS",
"AOS",
"BVN",
"COKE",
"NGD",
"POOL",
"AEIS",
"GAP",
"VNO-PM",
"MOG-B",
"COMP",
"WTFC",
"GNRC",
"BIO-B",
"OSK",
"EHC",
"TTMI",
"ALV",
"SOLS",
"WAL",
"ARE",
"EGO",
"TAP",
"DDS",
"BROS",
"MOG-A",
"REXR",
"AAL",
"KT",
"ORI",
"TTEK",
"AYI",
"SKM",
"ABVX",
"EGP",
"NCLH",
"ACI",
"HSIC",
"KNSL",
"IESC",
"MGM",
"UWMC",
"GTLS",
"ARWR",
"UMBF",
"AXSM",
"GS-PC",
"EDU",
"SFD",
"ONB",
"DINO",
"SAIA",
"RGEN",
"DOX",
"ALSN",
"KNX",
"STEP",
"LINE",
"APA",
"SITM",
"MOS",
"SWKS",
"AM",
"TFII",
"OGE",
"MORN",
"GDS",
"AMG",
"UEC",
"LUMN",
"GGAL",
"QBTS",
"TTAN",
"RZB",
"FRT",
"AGNCM",
"VMI",
"CFR",
"AGNCN",
"RRC",
"TTC",
"ZION",
"LKQ",
"PEGA",
"CAG",
"COLB",
"CUBE",
"AHR",
"VIPS",
"OR",
"AUR",
"GGB",
"UGI",
"IDCC",
"AMTM",
"PL",
"AGCO",
"CMC",
"LEVI",
"ADC",

]

# Tekrarları temizle
SYMBOLS = list(dict.fromkeys([s.strip().strip('"').strip(",") for s in SYMBOLS if s and str(s).strip()]))


# =======================
# 2) PARAMETRELER
# =======================
ATR_PERIOD = 10
MULTIPLIER = 3.0

DN_DIST_LIMIT = 0.02        # %2
MIN_DN_STREAK_DAYS = 2

LOOKBACK_1H = 48            # saat
LOOKBACK_4H = 96            # saat

TOP_SCORE_MIN = 3
TOP_N = 10
VOLUME_TOP_N = 10

FAST_MODE = True            # True: daha hızlı (sleep yok)

STATE_DIR = "state"
STATE_FILE = os.path.join(STATE_DIR, "signal_hash.txt")


# =======================
# 3) ZAMAN / TELEGRAM
# =======================
def now_tr_time_str() -> str:
    tr_time = datetime.now(timezone.utc) + timedelta(hours=3)
    return tr_time.strftime("%d.%m.%Y %H:%M")


def send_telegram_message(text: str) -> None:
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram ENV yok (TELEGRAM_TOKEN / TELEGRAM_CHAT_ID). Mesaj atlanıyor.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code != 200:
            print("Telegram hata:", r.status_code, r.text[:300])
    except Exception as e:
        print("Telegram exception:", e)


# =======================
# 4) STATE HASH
# =======================
def read_prev_hash() -> str:
    if not os.path.exists(STATE_FILE):
        return ""
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def write_new_hash(new_hash: str) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write(new_hash)


def stable_hash_from_dfs(df_en: pd.DataFrame, df_vol_filtered: pd.DataFrame) -> str:
    """
    Hash = EN_IYI + (ERKEN UYARI + HACİM EVET, EN_IYI hariç)
    İkisinden herhangi biri değişirse -> hash değişir -> Telegram gider
    """
    def df_to_str(df: pd.DataFrame, tag: str) -> str:
        if df is None or df.empty:
            return f"{tag}:EMPTY"
        cols = ["Hisse", "Son Kapanış", "MTF Skor", "DN Mesafe %", "Buy_1H", "Buy_4H", "DN Yakınlık Gün"]
        sub = df[cols].copy()
        lines = ["|".join(map(str, r)) for r in sub.itertuples(index=False)]
        return f"{tag}:" + "||".join(lines)

    s = df_to_str(df_en, "EN_IYI") + "\n" + df_to_str(df_vol_filtered, "HACIM_EVET_FILTERED")
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# =======================
# 5) VERİ ÇEKME
# =======================
def normalize_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    cols = ["Open", "High", "Low", "Close"]
    if "Volume" in df.columns:
        cols.append("Volume")

    need = {"Open", "High", "Low", "Close"}
    if not need.issubset(set(df.columns)):
        return pd.DataFrame()

    df = df[cols].copy()
    df.dropna(subset=["Open", "High", "Low", "Close"], inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


def safe_history(symbol: str, period: str, interval: str, max_tries: int = 4) -> pd.DataFrame:
    """
    interval SADECE: '1d', '1h', '4h'  (2h YOK!)
    """
    last_err = None
    for i in range(max_tries):
        try:
            sess = cffi_requests.Session(impersonate="chrome")
            t = yf.Ticker(symbol, session=sess)
            df = t.history(period=period, interval=interval, auto_adjust=False)
            df = normalize_ohlc(df)
            if not df.empty:
                return df
        except Exception as e:
            last_err = e
        time.sleep(0.6 + i * 0.4)
    raise RuntimeError(f"{symbol} veri alınamadı: {last_err}")


# =======================
# 6) SUPER TREND (Pine v4 mantığı)
# =======================
def supertrend_pine(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    h = df["High"].values
    l = df["Low"].values
    c = df["Close"].values
    hl2 = (h + l) / 2.0

    pc = np.roll(c, 1)
    pc[0] = c[0]

    tr = np.maximum.reduce([h - l, np.abs(h - pc), np.abs(l - pc)])
    atr = pd.Series(tr, index=df.index).ewm(
        alpha=1 / ATR_PERIOD, adjust=False, min_periods=ATR_PERIOD
    ).mean().values

    n = len(df)
    up = np.full(n, np.nan)
    dn = np.full(n, np.nan)
    trend = np.ones(n, dtype=int)

    for i in range(n):
        if np.isnan(atr[i]):
            continue

        up_raw = hl2[i] - MULTIPLIER * atr[i]
        dn_raw = hl2[i] + MULTIPLIER * atr[i]

        if i == 0:
            up[i], dn[i] = up_raw, dn_raw
            continue

        up_prev, dn_prev = up[i - 1], dn[i - 1]

        up[i] = max(up_raw, up_prev) if c[i - 1] > up_prev else up_raw
        dn[i] = min(dn_raw, dn_prev) if c[i - 1] < dn_prev else dn_raw

        if trend[i - 1] == -1 and c[i] > dn_prev:
            trend[i] = 1
        elif trend[i - 1] == 1 and c[i] < up_prev:
            trend[i] = -1
        else:
            trend[i] = trend[i - 1]

    df["Trend"] = trend
    df["DN"] = dn
    df["BUY"] = (df["Trend"] == 1) & (df["Trend"].shift(1) == -1)
    return df


# =======================
# 7) ERKEN UYARI + HACİM + MTF
# =======================
def dn_distance_pct(row: pd.Series) -> float:
    if pd.isna(row.get("DN")) or pd.isna(row.get("Close")):
        return np.nan
    return (float(row["DN"]) - float(row["Close"])) / float(row["Close"])


def dn_near_streak(out: pd.DataFrame) -> int:
    cnt = 0
    for i in range(len(out) - 1, -1, -1):
        r = out.iloc[i]
        if int(r["Trend"]) != -1:
            break
        d = dn_distance_pct(r)
        if np.isnan(d) or d < 0 or d > DN_DIST_LIMIT:
            break
        cnt += 1
    return cnt


def early_warning_daily(out: pd.DataFrame):
    if out is None or len(out) < 4:
        return False, np.nan, 0

    last = out.iloc[-1]
    if int(last["Trend"]) != -1 or pd.isna(last["DN"]):
        return False, np.nan, 0

    dist = dn_distance_pct(last)
    if np.isnan(dist) or not (0 <= dist <= DN_DIST_LIMIT):
        return False, dist, 0

    c0, c1, c2 = out["Close"].iloc[-1], out["Close"].iloc[-2], out["Close"].iloc[-3]
    if not (c0 > c1 > c2):
        return False, dist, 0

    streak = dn_near_streak(out)
    if streak < MIN_DN_STREAK_DAYS:
        return False, dist, streak

    return True, dist, streak


def volume_increase_flag(df_daily: pd.DataFrame) -> bool:
    if df_daily is None or df_daily.empty or "Volume" not in df_daily.columns:
        return False
    vol = df_daily["Volume"].dropna()
    if len(vol) < 26:
        return False
    last5 = vol.iloc[-5:].mean()
    prev20 = vol.iloc[-25:-5].mean()
    if np.isnan(prev20) or prev20 <= 0:
        return False
    return last5 > prev20


def recent_buy_on_tf(symbol: str, interval: str, lookback_hours: int) -> bool:
    """
    interval SADECE: 1h ve 4h (2h kesinlikle yok)
    """
    try:
        df = safe_history(symbol, period="10d", interval=interval)
        if df.empty or len(df) < ATR_PERIOD + 5:
            return False

        out = supertrend_pine(df)
        buys = out[out["BUY"]]
        if buys.empty:
            return False

        last_buy = pd.to_datetime(buys.index[-1])
        now_utc = pd.Timestamp.now("UTC")
        if last_buy.tzinfo is None:
            last_buy = last_buy.tz_localize("UTC")

        hours_ago = (now_utc - last_buy).total_seconds() / 3600.0
        return hours_ago <= lookback_hours
    except Exception:
        return False


def sort_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df.sort_values(by=["MTF Skor", "DN Mesafe %"], ascending=[False, True], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def build_lists():
    rows_en = []
    rows_vol_yes = []

    for symbol in SYMBOLS:
        try:
            # 1) önce günlükten ele
            df_d = safe_history(symbol, period="8y", interval="1d")
            if df_d.empty or len(df_d) < (ATR_PERIOD + 10):
                continue

            out_d = supertrend_pine(df_d)
            ok, dist, streak = early_warning_daily(out_d)
            if not ok:
                continue

            # Son kapanış
            last_close = float(df_d["Close"].iloc[-1])

            # 2) hacim
            vol_yes = volume_increase_flag(df_d)

            # 3) 1h/4h teyit (sadece geçenlere)
            score = 2
            buy_1h = recent_buy_on_tf(symbol, "1h", LOOKBACK_1H)
            buy_4h = recent_buy_on_tf(symbol, "4h", LOOKBACK_4H)
            if buy_1h:
                score += 1
            if buy_4h:
                score += 1

            row = {
                "Hisse": symbol.replace(".IS", ""),
                "Son Kapanış": round(last_close, 2),
                "MTF Skor": int(score),
                "DN Mesafe %": round(float(dist) * 100, 2),
                "Buy_1H": "Evet" if buy_1h else "Hayır",
                "Buy_4H": "Evet" if buy_4h else "Hayır",
                "DN Yakınlık Gün": int(streak),
            }

            if vol_yes:
                rows_vol_yes.append(row)

            if score >= TOP_SCORE_MIN:
                rows_en.append(row)

            if not FAST_MODE:
                time.sleep(0.15)

        except Exception:
            continue

    df_en = sort_df(pd.DataFrame(rows_en)).head(TOP_N)
    df_vol = sort_df(pd.DataFrame(rows_vol_yes)).head(VOLUME_TOP_N)
    return df_en, df_vol


def build_telegram_message(df_en: pd.DataFrame, df_vol_filtered: pd.DataFrame) -> str:
    ts = now_tr_time_str()
    msg = f"🕒 <i>{ts}</i>\n\n"

    msg += f"📈 <b>EN_IYI (Skor ≥ {TOP_SCORE_MIN})</b>\n"
    if df_en.empty:
        msg += "⏳ Yok\n\n"
    else:
        for _, r in df_en.iterrows():
            msg += (
                f"• <b>{r['Hisse']}</b> | Kapanış {r['Son Kapanış']} | Skor {r['MTF Skor']} | DN% {r['DN Mesafe %']} | "
                f"1H:{r['Buy_1H']} | 4H:{r['Buy_4H']} | Streak:{r['DN Yakınlık Gün']}\n"
            )
        msg += f"\nToplam: <b>{len(df_en)}</b>\n\n"

    msg += "🔥 <b>ERKEN UYARI + HACİM EVET (EN_IYI hariç)</b>\n"
    if df_vol_filtered.empty:
        msg += "⏳ Yok\n"
    else:
        for _, r in df_vol_filtered.iterrows():
            msg += (
                f"• <b>{r['Hisse']}</b> | Kapanış {r['Son Kapanış']} | Skor {r['MTF Skor']} | DN% {r['DN Mesafe %']} | "
                f"1H:{r['Buy_1H']} | 4H:{r['Buy_4H']} | Streak:{r['DN Yakınlık Gün']}\n"
            )
        msg += f"\nToplam: <b>{len(df_vol_filtered)}</b>\n"

    return msg


def main():
    df_en, df_vol = build_lists()

    # HACİM listesinden EN_IYI tekrarlarını çıkar
    en_set = set(df_en["Hisse"].tolist()) if (df_en is not None and not df_en.empty) else set()
    df_vol_filtered = df_vol[~df_vol["Hisse"].isin(en_set)].copy() if (df_vol is not None and not df_vol.empty) else pd.DataFrame()

    prev_hash = read_prev_hash()
    new_hash = stable_hash_from_dfs(df_en, df_vol_filtered)

    print("Prev hash:", prev_hash)
    print("New  hash:", new_hash)

    # İkisinden biri değişirse gönder
    if new_hash != prev_hash:
        msg = build_telegram_message(df_en, df_vol_filtered)
        send_telegram_message(msg)
        write_new_hash(new_hash)
        print("Değişim var → Telegram gönderildi, state güncellendi.")
    else:
        print("Değişim yok → Telegram gönderilmedi.")


if __name__ == "__main__":
    main()
