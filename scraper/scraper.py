import os
import re
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date
from ddgs import DDGS
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9",
}

# ── Ciudades ──────────────────────────────────────────────────
CIUDADES = [
    "Madrid","Barcelona","Valencia","Sevilla","Bilbao","Málaga","Zaragoza",
    "Murcia","Palma","Las Palmas","Alicante","Córdoba","Valladolid","Vigo",
    "Gijón","Granada","Pamplona","Salamanca","San Sebastián","Santander",
    "Toledo","Burgos","Oviedo","Albacete","Teruel","Cádiz","Huelva","Almería",
    "Donostia","Logroño","Castellón","Badajoz","Lleida","Tarragona","Girona",
    "Huesca","Lugo","Pontevedra","Ourense","A Coruña","Gandia","Benidorm",
]

MESES = {
    "ene":1,"enero":1,"feb":2,"febrero":2,"mar":3,"marzo":3,
    "abr":4,"abril":4,"may":5,"mayo":5,"jun":6,"junio":6,
    "jul":7,"julio":7,"ago":8,"agosto":8,"sep":9,"septiembre":9,
    "oct":10,"octubre":10,"nov":11,"noviembre":11,"dic":12,"diciembre":12,
    "jan":1,"january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

URLS_BLOQUEADAS = [
    "youtube.com","youtu.be","instagram.com","tiktok.com","twitter.com",
    "x.com","facebook.com","vk.com","rutube.ru","wikipedia.org","reddit.com",
    "pinkviral","mackolik","kinogo","rosserialls","latroupe","kaz-media",
]

PAISES_EXCLUIDOS = [
    "helsinki","finland","amsterdam","netherlands"," london","paris "," berlin",
    "new york","san francisco","toronto","ecuador","colombia","mexico ",
    "argentina","peru","chile","brasil","brazil","kenya","india ","china ",
    "japan","korea","australia","austria","poland","portugal","italy",
    "ukraine","turkey","sweden","norway","denmark","aalto university",
]

def es_url_valida(url):
    return not any(b in url.lower() for b in URLS_BLOQUEADAS)

def es_de_espana(texto):
    t = texto.lower()
    if any(p in t for p in PAISES_EXCLUIDOS):
        return False
    ok = ["españa","spain","madrid","barcelona","valencia","sevilla","bilbao",
          "málaga","malaga","zaragoza","granada","murcia","alicante","córdoba",
          ".es/","español","española","universit"]
    return any(w in t for w in ok)

def extraer_ciudad(texto):
    t = texto.lower()
    if any(w in t for w in ["online","virtual","remoto","remote","worldwide"]):
        return "Online"
    for c in CIUDADES:
        if c.lower() in t:
            return c
    return None

def extraer_fecha(texto):
    if not texto: return None
    t = texto.lower()
    for m in re.finditer(r'(202[5-7])-(\d{2})-(\d{2})', texto):
        try: return date(int(m.group(1)),int(m.group(2)),int(m.group(3))).isoformat()
        except: pass
    for m in re.finditer(r'(\d{1,2})\s+(?:de\s+)?([a-záéíóú]+)\s+(?:de\s+)?(202[5-7])', t):
        mes = MESES.get(m.group(2)[:3])
        if mes:
            try: return date(int(m.group(3)),mes,int(m.group(1))).isoformat()
            except: pass
    for m in re.finditer(r'([a-z]+)\s+(\d{1,2})(?:-\d+)?,?\s+(202[5-7])', t):
        mes = MESES.get(m.group(1)[:3])
        if mes:
            try: return date(int(m.group(3)),mes,int(m.group(2))).isoformat()
            except: pass
    for m in re.finditer(r'(\d{1,2})[/\-](\d{1,2})[/\-](202[5-7])', texto):
        try: return date(int(m.group(3)),int(m.group(2)),int(m.group(1))).isoformat()
        except: pass
    return None

def es_hackathon(titulo):
    t = titulo.lower()
    return any(k in t for k in ["hackathon","hackaton","hackatón","hackfest","datathon"])

def limpiar_html(html):
    return re.sub(r'\s+',' ', re.sub(r'<[^>]+>',' ', html)).strip()


# ══════════════════════════════════════════════════════════════
# FUENTE 1: DEVPOST RSS (público, sin bloqueo)
# ══════════════════════════════════════════════════════════════
def scrape_devpost_rss():
    print("\n📡 Fuente 1: Devpost RSS...")
    eventos = []
    feeds = [
        "https://devpost.com/hackathons.rss?challenge_type=all&status%5B%5D=upcoming&order_by=deadline&per_page=50",
        "https://devpost.com/hackathons.rss?challenge_type=all&status%5B%5D=upcoming&order_by=deadline&per_page=50&location=Spain",
    ]
    vistos = set()
    for feed_url in feeds:
        try:
            r = requests.get(feed_url, headers=HEADERS, timeout=12)
            root = ET.fromstring(r.content)
            items = root.findall('.//item')
            print(f"   → {len(items)} items en feed")
            for item in items:
                titulo = (item.findtext('title') or '').strip()
                url    = (item.findtext('link')  or '').strip()
                desc   = limpiar_html(item.findtext('description') or '')
                pubdate = item.findtext('pubDate') or ''

                if not titulo or not url or url in vistos: continue
                vistos.add(url)

                texto = f"{titulo} {desc} {pubdate}"
                ciudad = extraer_ciudad(texto)
                fecha  = extraer_fecha(texto) or extraer_fecha(pubdate)

                eventos.append({
                    "nombre":         titulo[:300],
                    "descripcion":    desc[:1000],
                    "url":            url,
                    "ciudad":         ciudad,
                    "online":         ciudad=="Online",
                    "fecha_inicio":   fecha,
                    "fuente":         "Devpost",
                    "fecha_scraping": datetime.now().isoformat(),
                })
                print(f"   ✓ {titulo[:55]} | {fecha or '?'} | {ciudad or '?'}")
            time.sleep(1)
        except Exception as e:
            print(f"   ⚠️ {e}")
    print(f"   → {len(eventos)} de Devpost RSS")
    return eventos


# ══════════════════════════════════════════════════════════════
# FUENTE 2: HACKATHONSPAIN.COM (scraping mejorado)
# ══════════════════════════════════════════════════════════════
def scrape_hackathonspain():
    print("\n📡 Fuente 2: HackathonSpain.com...")
    eventos = []
    try:
        r = requests.get("https://hackathonspain.com/calendario/", headers=HEADERS, timeout=15)
        html = r.text

        # Buscar todos los hrefs que apunten a eventos del calendario
        # Admite URLs absolutas y relativas
        all_hrefs = re.findall(r'href=["\']([^"\']+)["\']', html)
        slugs = set()
        for href in all_hrefs:
            # URLs tipo /calendario/nombre/ o https://hackathonspain.com/calendario/nombre/
            m = re.search(r'/calendario/([^/?#"\']{3,}?)/?$', href)
            if m:
                slug = m.group(1)
                if slug not in ('', 'calendario', 'page'):
                    slugs.add(slug)

        print(f"   → {len(slugs)} eventos encontrados en calendario")

        for slug in list(slugs)[:35]:
            url_ev = f"https://hackathonspain.com/calendario/{slug}/"
            try:
                r2 = requests.get(url_ev, headers=HEADERS, timeout=10)
                if r2.status_code != 200: continue
                html2 = r2.text
                texto = limpiar_html(html2)

                # Nombre del evento desde <h1>
                h1 = re.search(r'<h1[^>]*>(.*?)</h1>', html2, re.DOTALL|re.IGNORECASE)
                nombre = limpiar_html(h1.group(1)) if h1 else slug.replace('-',' ').title()

                if not nombre or not es_hackathon(nombre): continue

                fecha  = extraer_fecha(texto[:5000])
                ciudad = extraer_ciudad(f"{nombre} {texto[:2000]}")

                eventos.append({
                    "nombre":         nombre[:300],
                    "descripcion":    texto[:800],
                    "url":            url_ev,
                    "ciudad":         ciudad,
                    "online":         ciudad=="Online",
                    "fecha_inicio":   fecha,
                    "fuente":         "HackathonSpain",
                    "fecha_scraping": datetime.now().isoformat(),
                })
                print(f"   ✓ {nombre[:55]} | {fecha or '?'} | {ciudad or '?'}")
                time.sleep(0.5)
            except Exception as e:
                print(f"   ⚠️ {slug}: {e}")
    except Exception as e:
        print(f"   ⚠️ Error general: {e}")

    print(f"   → {len(eventos)} de HackathonSpain")
    return eventos


# ══════════════════════════════════════════════════════════════
# FUENTE 3: DUCKDUCKGO
# ══════════════════════════════════════════════════════════════
QUERIES = [
    "hackathon España 2026 inscripción",
    "hackathon Madrid Barcelona 2026",
    "hackathon Valencia Sevilla Bilbao 2026",
    "hackathon Málaga Granada Zaragoza 2026",
    "hackathon universitario España 2026",
    "hackathon IA inteligencia artificial España 2026",
    "NASA hackathon España 2026",
    "convocatoria hackathon España 2026",
]

def scrape_duckduckgo():
    print("\n📡 Fuente 3: DuckDuckGo...")
    eventos = []
    vistos = set()
    with DDGS() as ddgs:
        for query in QUERIES:
            try:
                hits = ddgs.text(query, max_results=10)
                for r in hits:
                    url    = r.get("href","")
                    titulo = r.get("title","")
                    cuerpo = r.get("body","")
                    texto  = f"{titulo} {cuerpo} {url}"

                    if not url or url in vistos:    continue
                    if not es_url_valida(url):      continue
                    if not es_hackathon(titulo):    continue
                    if not es_de_espana(texto):     continue
                    vistos.add(url)

                    ciudad = extraer_ciudad(texto)
                    fecha  = extraer_fecha(f"{titulo} {cuerpo}")

                    # Intentar obtener fecha visitando la página .es
                    if not fecha and (".es" in url or "hackathon" in url.lower()):
                        try:
                            rp = requests.get(url, headers=HEADERS, timeout=7)
                            if rp.status_code == 200:
                                fecha = extraer_fecha(rp.text[:40000])
                            time.sleep(0.3)
                        except: pass

                    eventos.append({
                        "nombre":         titulo[:300],
                        "descripcion":    cuerpo[:1000],
                        "url":            url,
                        "ciudad":         ciudad,
                        "online":         ciudad=="Online",
                        "fecha_inicio":   fecha,
                        "fuente":         "DuckDuckGo",
                        "fecha_scraping": datetime.now().isoformat(),
                    })
                    print(f"   ✓ {titulo[:55]} | {fecha or '?'} | {ciudad or '?'}")
            except Exception as e:
                print(f"   ⚠️ '{query}': {e}")
    print(f"   → {len(eventos)} de DuckDuckGo")
    return eventos


# ══════════════════════════════════════════════════════════════
# GUARDAR
# ══════════════════════════════════════════════════════════════
def guardar(eventos):
    nuevos = actualizados = 0
    for ev in eventos:
        if not ev.get("nombre") or not ev.get("url"): continue
        try:
            ex = supabase.table("hackathons")\
                .select("id,ciudad,fecha_inicio").eq("url",ev["url"]).execute()
            if not ex.data:
                supabase.table("hackathons").insert(ev).execute()
                nuevos += 1
                print(f"   ✅ {ev['nombre'][:65]}")
            else:
                row = ex.data[0]
                upd = {}
                if ev.get("ciudad")       and not row.get("ciudad"):       upd["ciudad"]       = ev["ciudad"]
                if ev.get("fecha_inicio") and not row.get("fecha_inicio"): upd["fecha_inicio"] = ev["fecha_inicio"]
                if upd:
                    supabase.table("hackathons").update(upd).eq("id",row["id"]).execute()
                    actualizados += 1
                    print(f"   🔄 {ev['nombre'][:65]}")
        except Exception as e:
            print(f"   ❌ {ev['nombre'][:40]}: {e}")
    print(f"\n── Resumen ──  ✅ Nuevos: {nuevos}  🔄 Actualizados: {actualizados}")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 hackathons.es scraper v5")
    todos  = scrape_devpost_rss()
    todos += scrape_hackathonspain()
    todos += scrape_duckduckgo()

    vistos, unicos = set(), []
    for ev in todos:
        if ev["url"] not in vistos:
            vistos.add(ev["url"])
            unicos.append(ev)

    print(f"\n📦 Total únicos: {len(unicos)}")
    guardar(unicos)
    print("\n✅ Listo")
