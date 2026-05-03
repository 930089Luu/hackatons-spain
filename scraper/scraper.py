import os
import re
import json
import time
import requests
from datetime import datetime, date
from ddgs import DDGS
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

CIUDADES = [
    "Madrid","Barcelona","Valencia","Sevilla","Bilbao","Málaga","Zaragoza",
    "Murcia","Palma","Las Palmas","Alicante","Córdoba","Valladolid","Vigo",
    "Gijón","Granada","Pamplona","Salamanca","San Sebastián","Santander",
    "Toledo","Burgos","Oviedo","Albacete","Teruel","Cádiz","Huelva","Almería",
    "Donostia","Logroño","Castellón","Badajoz","Lleida","Tarragona","Girona",
    "Huesca","Lugo","Pontevedra","Ourense","A Coruña","Gandia","Elche",
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
    "pinkviral","mackolik","kinogo","latroupe","kaz-media","scrum","apify.com",
]

PAISES_EXCLUIDOS = [
    "helsinki","finland","amsterdam","netherlands"," london "," paris ",
    " berlin ","new york","san francisco","toronto","ecuador","colombia",
    "mexico ","argentina","peru","chile","brasil","brazil","kenya",
    "india ","china ","japan","korea","australia","austria","poland",
    "italy ","ukraine","turkey","sweden","norway","denmark",
]

def es_url_valida(url):
    return not any(b in url.lower() for b in URLS_BLOQUEADAS)

def es_de_espana(texto):
    t = texto.lower()
    if any(p in t for p in PAISES_EXCLUIDOS): return False
    ok = ["españa","spain","madrid","barcelona","valencia","sevilla","bilbao",
          "málaga","malaga","zaragoza","granada","murcia","alicante",".es/",
          "español","española","universit","hackathonspain"]
    return any(w in t for w in ok)

def extraer_ciudad(texto):
    t = texto.lower()
    if any(w in t for w in ["online","virtual","remoto","remote","worldwide"]): return "Online"
    for c in CIUDADES:
        if c.lower() in t: return c
    return None

def extraer_fecha_texto(texto):
    """Extrae fecha de un string de texto con regex."""
    if not texto: return None
    t = texto.lower()
    # ISO
    for m in re.finditer(r'(202[5-7])-(\d{2})-(\d{2})', texto):
        try: return date(int(m.group(1)),int(m.group(2)),int(m.group(3))).isoformat()
        except: pass
    # "25 de mayo de 2026"
    for m in re.finditer(r'(\d{1,2})\s+(?:de\s+)?([a-záéíóú]{3,})\s+(?:de\s+)?(202[5-7])', t):
        mes = MESES.get(m.group(2)[:3])
        if mes:
            try: return date(int(m.group(3)),mes,int(m.group(1))).isoformat()
            except: pass
    # "April 24-26, 2026"
    for m in re.finditer(r'([a-z]{3,})\s+(\d{1,2})(?:\s*[-–]\s*\d+)?,?\s+(202[5-7])', t):
        mes = MESES.get(m.group(1)[:3])
        if mes:
            try: return date(int(m.group(3)),mes,int(m.group(2))).isoformat()
            except: pass
    # "25/04/2026"
    for m in re.finditer(r'(\d{1,2})[/\-](\d{1,2})[/\-](202[5-7])', texto):
        try: return date(int(m.group(3)),int(m.group(2)),int(m.group(1))).isoformat()
        except: pass
    # Año en contexto + mes: "2026 | April 24"
    for m in re.finditer(r'(202[5-7])\W{1,15}([a-z]{3,})\s+(\d{1,2})', t):
        mes = MESES.get(m.group(2)[:3])
        if mes:
            try: return date(int(m.group(1)),mes,int(m.group(3))).isoformat()
            except: pass
    return None

def limpiar(html):
    return re.sub(r'\s+',' ', re.sub(r'<[^>]+>',' ',html)).strip()

# ═══════════════════════════════════════════════════════
# EXTRACCIÓN DE FECHA: JSON-LD PRIMERO (el más fiable)
# ═══════════════════════════════════════════════════════
def extraer_fecha_html(html):
    """
    Extrae fecha de una página HTML.
    Prioridad: JSON-LD > meta tags > <time> > regex en texto.
    JSON-LD es el estándar web para datos de eventos (Schema.org).
    """
    # 1. JSON-LD — MÁXIMA FIABILIDAD
    for script in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    ):
        try:
            raw = script.group(1).strip()
            data = json.loads(raw)
            items = data if isinstance(data, list) else [data]
            for item in items:
                # A veces está anidado en @graph
                if '@graph' in item:
                    items += item['@graph']
                tipo = str(item.get('@type',''))
                if any(t in tipo for t in ('Event','Hackathon','Education','Social','Festival','Course')):
                    for campo in ('startDate','datePublished','dateCreated','startTime'):
                        val = item.get(campo,'')
                        if val:
                            f = extraer_fecha_texto(str(val)[:25])
                            if f: return f
        except: pass

    # 2. Meta tags con fechas de eventos
    for pat in [
        r'<meta[^>]+(?:property|name)=["\'][^"\']*(?:start|published|event)[^"\']*["\'][^>]+content=["\']([^"\']+)["\']',
        r'<time[^>]+datetime=["\']([^"\']+)["\']',
        r'data-start(?:date|time)=["\']([^"\']+)["\']',
        r'startDate["\s:]+["\']([^"\']+)["\']',
    ]:
        for m in re.finditer(pat, html, re.IGNORECASE):
            f = extraer_fecha_texto(m.group(1))
            if f: return f

    # 3. Regex en texto plano (último recurso)
    return extraer_fecha_texto(limpiar(html[:80000]))

def fetch_y_fecha(url):
    """Visita la URL y extrae fecha con JSON-LD + regex."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=9, allow_redirects=True)
        if r.status_code == 200:
            return extraer_fecha_html(r.text)
    except: pass
    return None

def es_hackathon(titulo):
    t = titulo.lower()
    return any(k in t for k in ["hackathon","hackaton","hackatón","hackfest","datathon","hack the","hack for"])


# ══════════════════════════════════════════════════════════
# FUENTE 1: EVENTBRITE ESPAÑA (HTML estático, tiene JSON-LD)
# ══════════════════════════════════════════════════════════
def scrape_eventbrite():
    print("\n📡 Fuente 1: Eventbrite España...")
    eventos = []
    urls = [
        "https://www.eventbrite.es/d/spain/hackathon/",
        "https://www.eventbrite.es/d/spain--madrid/hackathon/",
        "https://www.eventbrite.es/d/spain--barcelona/hackathon/",
    ]
    vistos = set()
    for url_base in urls:
        try:
            r = requests.get(url_base, headers=HEADERS, timeout=12)
            if r.status_code != 200: continue
            html = r.text
            # Eventbrite incluye JSON-LD con todos los eventos en la página
            # También buscar links a eventos individuales
            event_urls = re.findall(r'href=["\'](https://www\.eventbrite\.es/e/[^"\'?\s]+)', html)
            event_urls += re.findall(r'href=["\'](https://www\.eventbrite\.com/e/[^"\'?\s]+)', html)
            event_urls = [u for u in set(event_urls) if u not in vistos]

            for eu in event_urls[:20]:
                vistos.add(eu)
                try:
                    r2 = requests.get(eu, headers=HEADERS, timeout=10)
                    if r2.status_code != 200: continue
                    html2 = r2.text
                    texto = limpiar(html2)

                    # Título
                    h1 = re.search(r'<h1[^>]*>(.*?)</h1>', html2, re.DOTALL|re.IGNORECASE)
                    nombre = limpiar(h1.group(1)) if h1 else ""
                    if not nombre or not es_hackathon(nombre): continue
                    if not es_de_espana(f"{nombre} {texto[:2000]}"): continue

                    fecha  = extraer_fecha_html(html2)
                    ciudad = extraer_ciudad(f"{nombre} {texto[:3000]}")

                    eventos.append({
                        "nombre":nombre[:300],"descripcion":texto[:800],
                        "url":eu,"ciudad":ciudad,"online":ciudad=="Online",
                        "fecha_inicio":fecha,"fuente":"Eventbrite",
                        "fecha_scraping":datetime.now().isoformat(),
                    })
                    print(f"   ✓ {nombre[:55]} | {fecha or '?'} | {ciudad or '?'}")
                    time.sleep(0.4)
                except: pass
        except Exception as e:
            print(f"   ⚠️ {e}")
    print(f"   → {len(eventos)} de Eventbrite")
    return eventos


# ══════════════════════════════════════════════════════════
# FUENTE 2: DUCKDUCKGO + fetch JSON-LD en cada página
# ══════════════════════════════════════════════════════════
QUERIES = [
    "hackathon España 2026 inscripción fecha",
    "hackathon Madrid 2026",
    "hackathon Barcelona 2026",
    "hackathon Valencia Sevilla Bilbao 2026",
    "hackathon Málaga Granada Zaragoza 2026",
    "hackathon universitario España 2026",
    "hackathon inteligencia artificial España 2026",
    "NASA Space Apps hackathon España 2026",
    "hackathon startups innovación España 2026",
]

def scrape_duckduckgo():
    print("\n📡 Fuente 2: DuckDuckGo...")
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
                    fecha  = extraer_fecha_texto(f"{titulo} {cuerpo}")

                    # Siempre intentar JSON-LD de la página
                    if not fecha:
                        fecha = fetch_y_fecha(url)
                        time.sleep(0.3)

                    eventos.append({
                        "nombre":titulo[:300],"descripcion":cuerpo[:1000],
                        "url":url,"ciudad":ciudad,"online":ciudad=="Online",
                        "fecha_inicio":fecha,"fuente":"DuckDuckGo",
                        "fecha_scraping":datetime.now().isoformat(),
                    })
                    print(f"   ✓ {titulo[:55]} | {fecha or '?'} | {ciudad or '?'}")
            except Exception as e:
                print(f"   ⚠️ '{query}': {e}")
    print(f"   → {len(eventos)} de DuckDuckGo")
    return eventos


# ══════════════════════════════════════════════════════════
# ACTUALIZAR REGISTROS EXISTENTES SIN FECHA (con JSON-LD)
# ══════════════════════════════════════════════════════════
def actualizar_sin_fecha():
    print("\n🔄 Actualizando eventos sin fecha con JSON-LD...")
    try:
        sin_fecha = supabase.table("hackathons")\
            .select("id,nombre,url")\
            .is_("fecha_inicio","null")\
            .not_.is_("url","null")\
            .limit(60).execute()

        actualizados = 0
        for row in sin_fecha.data or []:
            url = row.get("url","")
            if not url or not url.startswith("http"): continue
            try:
                fecha = fetch_y_fecha(url)
                if fecha:
                    supabase.table("hackathons")\
                        .update({"fecha_inicio":fecha})\
                        .eq("id",row["id"]).execute()
                    actualizados += 1
                    print(f"   🔄 {row['nombre'][:60]} → {fecha}")
                time.sleep(0.4)
            except: pass
        print(f"   → {actualizados} registros actualizados")
    except Exception as e:
        print(f"   ⚠️ Error: {e}")


# ══════════════════════════════════════════════════════════
# GUARDAR
# ══════════════════════════════════════════════════════════
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
    print(f"\n── ✅ Nuevos: {nuevos}  🔄 Actualizados: {actualizados}")


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 hackathons.es scraper v8 (JSON-LD)")

    # Paso 1: Actualizar registros existentes sin fecha
    actualizar_sin_fecha()

    # Paso 2: Buscar nuevos eventos
    todos  = scrape_eventbrite()
    todos += scrape_duckduckgo()

    vistos, unicos = set(), []
    for ev in todos:
        if ev["url"] not in vistos:
            vistos.add(ev["url"])
            unicos.append(ev)

    print(f"\n📦 Total únicos: {len(unicos)}")
    guardar(unicos)
    print("\n✅ Listo")
