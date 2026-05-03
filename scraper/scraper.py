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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

CIUDADES = [
    "Madrid","Barcelona","Valencia","Sevilla","Bilbao","Málaga","Zaragoza",
    "Murcia","Palma","Las Palmas","Alicante","Córdoba","Valladolid","Vigo",
    "Gijón","Granada","Pamplona","Salamanca","San Sebastián","Santander",
    "Toledo","Burgos","Oviedo","Albacete","Teruel","Cádiz","Huelva","Almería",
    "Donostia","Logroño","Castellón","Badajoz","Lleida","Tarragona","Girona",
    "Huesca","Lugo","Pontevedra","Ourense","A Coruña","Gandia","Benidorm",
    "Elche","Cartagena","Jerez","Alcalá","Alcobendas","Getafe","Leganés",
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
    "airmeet.com","thechangingbooth","apilayer","apiworld","devnetwork",
]

PAISES_EXCLUIDOS = [
    "helsinki","finland","amsterdam","netherlands"," london ","paris ",
    " berlin ","new york","san francisco","toronto","ecuador","colombia",
    "mexico ","argentina","peru","chile","brasil","brazil","kenya","india ",
    "china ","japan","korea","australia","austria","poland","portugal",
    "italy ","ukraine","turkey","sweden","norway","denmark","aalto university",
    "scotland","ireland","belgium","switzerland","czech","romania","hungary",
]

def es_url_valida(url):
    return not any(b in url.lower() for b in URLS_BLOQUEADAS)

def es_de_espana(texto):
    t = texto.lower()
    if any(p in t for p in PAISES_EXCLUIDOS):
        return False
    ok = ["españa","spain","madrid","barcelona","valencia","sevilla","bilbao",
          "málaga","malaga","zaragoza","granada","murcia","alicante","córdoba",
          ".es/","español","española","universit","hackathonspain"]
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
    """Extrae la primera fecha válida de 2025-2027 del texto."""
    if not texto: return None
    t = texto.lower()

    # 1. ISO: "2026-05-25"
    for m in re.finditer(r'(202[5-7])-(\d{2})-(\d{2})', texto):
        try: return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
        except: pass

    # 2. "25 de mayo de 2026" / "25 mayo 2026"
    for m in re.finditer(r'(\d{1,2})\s+(?:de\s+)?([a-záéíóú]{3,})\s+(?:de\s+)?(202[5-7])', t):
        mes = MESES.get(m.group(2)[:3])
        if mes:
            try: return date(int(m.group(3)), mes, int(m.group(1))).isoformat()
            except: pass

    # 3. "April 24-26, 2026" / "April 24, 2026" (año después)
    for m in re.finditer(r'([a-z]{3,})\s+(\d{1,2})(?:-\d+)?,?\s+(202[5-7])', t):
        mes = MESES.get(m.group(1)[:3])
        if mes:
            try: return date(int(m.group(3)), mes, int(m.group(2))).isoformat()
            except: pass

    # 4. *** NUEVO *** "2026 | April 24-26" / "2026 April 24" (año antes)
    for m in re.finditer(r'(202[5-7])[^\w]{0,10}([a-z]{3,})\s+(\d{1,2})', t):
        mes = MESES.get(m.group(2)[:3])
        if mes:
            try: return date(int(m.group(1)), mes, int(m.group(3))).isoformat()
            except: pass

    # 5. "24/04/2026" / "24-04-2026"
    for m in re.finditer(r'(\d{1,2})[/\-](\d{1,2})[/\-](202[5-7])', texto):
        try: return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
        except: pass

    # 6. *** NUEVO *** "24 y 25 de abril de 2026"
    for m in re.finditer(r'(\d{1,2})\s+y\s+\d{1,2}\s+de\s+([a-záéíóú]{3,})\s+(?:de\s+)?(202[5-7])', t):
        mes = MESES.get(m.group(2)[:3])
        if mes:
            try: return date(int(m.group(3)), mes, int(m.group(1))).isoformat()
            except: pass

    return None

def es_hackathon(titulo):
    t = titulo.lower()
    return any(k in t for k in ["hackathon","hackaton","hackatón","hackfest","datathon","hack the","hack for"])

def limpiar_html(html):
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', html)).strip()


# ══════════════════════════════════════════════════════════════
# FUENTE 1: HACKATHONSPAIN — via Sitemap XML
# ══════════════════════════════════════════════════════════════
def scrape_hackathonspain():
    print("\n📡 Fuente 1: HackathonSpain.com (sitemap)...")
    eventos = []
    event_urls = []
    try:
        # Intentar sitemap.xml
        for sm_url in [
            "https://hackathonspain.com/sitemap.xml",
            "https://hackathonspain.com/sitemap_index.xml",
            "https://hackathonspain.com/post-sitemap.xml",
        ]:
            try:
                r = requests.get(sm_url, headers=HEADERS, timeout=10)
                if r.status_code == 200 and "<url" in r.text:
                    root = ET.fromstring(r.content)
                    ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                    # Buscar URLs de eventos del calendario
                    for loc in root.findall('.//sm:loc', ns) or root.findall('.//loc'):
                        u = loc.text or ''
                        if '/calendario/' in u and u.count('/') >= 5:
                            event_urls.append(u)
                    if event_urls:
                        print(f"   → {len(event_urls)} URLs en sitemap: {sm_url}")
                        break
            except: pass

        # Si no hay sitemap, parsear el HTML del calendario
        if not event_urls:
            r = requests.get("https://hackathonspain.com/calendario/", headers=HEADERS, timeout=15)
            html = r.text
            # Buscar todas las URLs del calendario en el HTML
            for href in re.findall(r'href=["\']([^"\']+)["\']', html):
                if '/calendario/' in href:
                    url_clean = href.split('?')[0].rstrip('/')
                    if url_clean.count('/') >= 4 and not url_clean.endswith('/calendario'):
                        full = href if href.startswith('http') else f"https://hackathonspain.com{href}"
                        event_urls.append(full)
            event_urls = list(set(event_urls))
            print(f"   → {len(event_urls)} URLs encontradas en HTML")

        # Visitar cada página de evento
        for url_ev in event_urls[:40]:
            try:
                r2 = requests.get(url_ev, headers=HEADERS, timeout=10)
                if r2.status_code != 200: continue
                html2 = r2.text
                texto = limpiar_html(html2)

                h1 = re.search(r'<h1[^>]*>(.*?)</h1>', html2, re.DOTALL|re.IGNORECASE)
                nombre = limpiar_html(h1.group(1)) if h1 else ""
                if not nombre or not es_hackathon(nombre): continue

                fecha  = extraer_fecha(texto[:8000])
                ciudad = extraer_ciudad(f"{nombre} {texto[:3000]}")

                eventos.append({
                    "nombre":         nombre[:300],
                    "descripcion":    texto[:800],
                    "url":            url_ev,
                    "ciudad":         ciudad,
                    "online":         ciudad == "Online",
                    "fecha_inicio":   fecha,
                    "fuente":         "HackathonSpain",
                    "fecha_scraping": datetime.now().isoformat(),
                })
                print(f"   ✓ {nombre[:55]} | {fecha or '?'} | {ciudad or '?'}")
                time.sleep(0.4)
            except Exception as e:
                print(f"   ⚠️ {url_ev[-40:]}: {e}")

    except Exception as e:
        print(f"   ⚠️ Error general: {e}")

    print(f"   → {len(eventos)} de HackathonSpain")
    return eventos


# ══════════════════════════════════════════════════════════════
# FUENTE 2: DEV.EVENTS (scraping directo, fechas estructuradas)
# ══════════════════════════════════════════════════════════════
def scrape_dev_events():
    print("\n📡 Fuente 2: dev.events/hackathons/EU/ES...")
    eventos = []
    try:
        r = requests.get("https://dev.events/hackathons/EU/ES", headers=HEADERS, timeout=15)
        html = r.text
        texto = limpiar_html(html)

        # Buscar bloques de eventos con su URL
        # dev.events usa patrones como /e/nombre-evento
        event_links = re.findall(r'href=["\'](/e/[^"\']+)["\']', html)
        event_links = list(set(event_links))[:30]

        for path in event_links:
            url_ev = f"https://dev.events{path}"
            try:
                r2 = requests.get(url_ev, headers=HEADERS, timeout=10)
                if r2.status_code != 200: continue
                html2 = r2.text
                texto2 = limpiar_html(html2)

                h1 = re.search(r'<h1[^>]*>(.*?)</h1>', html2, re.DOTALL|re.IGNORECASE)
                nombre = limpiar_html(h1.group(1)) if h1 else ""
                if not nombre or not es_hackathon(nombre): continue
                if not es_de_espana(f"{nombre} {texto2[:2000]}"): continue

                fecha  = extraer_fecha(texto2[:5000])
                ciudad = extraer_ciudad(f"{nombre} {texto2[:2000]}")

                eventos.append({
                    "nombre":         nombre[:300],
                    "descripcion":    texto2[:800],
                    "url":            url_ev,
                    "ciudad":         ciudad,
                    "online":         ciudad == "Online",
                    "fecha_inicio":   fecha,
                    "fuente":         "DevEvents",
                    "fecha_scraping": datetime.now().isoformat(),
                })
                print(f"   ✓ {nombre[:55]} | {fecha or '?'} | {ciudad or '?'}")
                time.sleep(0.4)
            except: pass

    except Exception as e:
        print(f"   ⚠️ Error: {e}")

    print(f"   → {len(eventos)} de dev.events")
    return eventos


# ══════════════════════════════════════════════════════════════
# FUENTE 3: DUCKDUCKGO (filtrado)
# ══════════════════════════════════════════════════════════════
QUERIES = [
    "hackathon España 2026 inscripción fecha",
    "hackathon Madrid 2026 fecha",
    "hackathon Barcelona 2026 fecha",
    "hackathon Valencia Sevilla Bilbao 2026",
    "hackathon Málaga Granada Zaragoza 2026",
    "hackathon universitario España 2026",
    "hackathon IA España 2026",
    "NASA Space Apps hackathon España 2026",
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

                    if not url or url in vistos: continue
                    if not es_url_valida(url):   continue
                    if not es_hackathon(titulo): continue
                    if not es_de_espana(texto):  continue
                    vistos.add(url)

                    ciudad = extraer_ciudad(texto)
                    fecha  = extraer_fecha(f"{titulo} {cuerpo}")

                    # Buscar fecha visitando la página si no tenemos
                    if not fecha:
                        try:
                            rp = requests.get(url, headers=HEADERS, timeout=7)
                            if rp.status_code == 200:
                                fecha = extraer_fecha(rp.text[:50000])
                            time.sleep(0.3)
                        except: pass

                    eventos.append({
                        "nombre":         titulo[:300],
                        "descripcion":    cuerpo[:1000],
                        "url":            url,
                        "ciudad":         ciudad,
                        "online":         ciudad == "Online",
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
                .select("id,ciudad,fecha_inicio").eq("url", ev["url"]).execute()
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
                    supabase.table("hackathons").update(upd).eq("id", row["id"]).execute()
                    actualizados += 1
                    print(f"   🔄 {ev['nombre'][:65]}")
        except Exception as e:
            print(f"   ❌ {ev['nombre'][:40]}: {e}")
    print(f"\n── ✅ Nuevos: {nuevos}  🔄 Actualizados: {actualizados}")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 hackathons.es scraper v6")
    todos  = scrape_hackathonspain()
    todos += scrape_dev_events()
    todos += scrape_duckduckgo()

    vistos, unicos = set(), []
    for ev in todos:
        if ev["url"] not in vistos:
            vistos.add(ev["url"])
            unicos.append(ev)

    print(f"\n📦 Total únicos: {len(unicos)}")
    guardar(unicos)
    print("\n✅ Listo")
