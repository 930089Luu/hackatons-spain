import os
import re
import time
import requests
from datetime import datetime, date
from ddgs import DDGS
from supabase import create_client

# ── Credenciales ──────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

# ── Ciudades ──────────────────────────────────────────────────
CIUDADES = [
    "Madrid","Barcelona","Valencia","Sevilla","Bilbao","Málaga","Zaragoza",
    "Murcia","Palma","Las Palmas","Alicante","Córdoba","Valladolid","Vigo",
    "Gijón","Granada","Pamplona","Salamanca","San Sebastián","Santander",
    "Toledo","Burgos","Oviedo","Albacete","Teruel","Cádiz","Huelva","Almería",
    "Donostia","Logroño","Castellón","Badajoz","Lleida","Tarragona","Girona",
    "Huesca","Lugo","Pontevedra","Ourense","Ferrol","A Coruña","Gandia",
]

MESES = {
    "ene":1,"enero":1,"feb":2,"febrero":2,"mar":3,"marzo":3,
    "abr":4,"abril":4,"may":5,"mayo":5,"jun":6,"junio":6,
    "jul":7,"julio":7,"ago":8,"agosto":8,"sep":9,"septiembre":9,
    "oct":10,"octubre":10,"nov":11,"noviembre":11,"dic":12,"diciembre":12,
    "jan":1,"january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

# URLs a ignorar siempre
URLS_BLOQUEADAS = [
    "youtube.com","youtu.be","instagram.com","tiktok.com","twitter.com",
    "x.com","facebook.com","linkedin.com","vk.com","rutube.ru","wikipedia.org",
    "reddit.com","medium.com","pinkviral","mackolik.com","kinogo","rosserialls",
    "latroupe.com","arsiv.","kaz-media","ecosistema","pymes","cordis.europa",
]

# Palabras que indican que NO es de España
PAISES_EXCLUIDOS = [
    "helsinki","finland","finland","amsterdam","netherlands","london","uk ",
    "paris","france","berlin","germany","new york","san francisco","toronto",
    "ecuador","colombia","mexico","argentina","peru","chile","brasil","brazil",
    "kenya","nigeria","india","china","japan","korea","australia",
]

def es_url_valida(url):
    u = url.lower()
    return not any(b in u for b in URLS_BLOQUEADAS)

def es_de_espana(texto):
    t = texto.lower()
    # Si menciona un país extranjero claramente, excluir
    if any(p in t for p in PAISES_EXCLUIDOS):
        return False
    # Debe mencionar España, una ciudad española, o tener dominio .es
    spain_words = ["españa","spain","madrid","barcelona","valencia","sevilla",
                   "bilbao","málaga","malaga","zaragoza","granada","murcia",
                   ".es/","spanish","español","española"]
    return any(w in t for w in spain_words)

def extraer_ciudad(texto):
    t = texto.lower()
    if any(w in t for w in ["online","virtual","remoto","remote","worldwide"]):
        return "Online"
    for ciudad in CIUDADES:
        if ciudad.lower() in t:
            return ciudad
    return None

def extraer_fecha(texto):
    if not texto:
        return None
    t = texto.lower()
    # ISO: "2026-05-25"
    for m in re.finditer(r'(202[5-7])-(\d{2})-(\d{2})', texto):
        try: return date(int(m.group(1)),int(m.group(2)),int(m.group(3))).isoformat()
        except: pass
    # "25 de mayo de 2026"
    for m in re.finditer(r'(\d{1,2})\s+(?:de\s+)?([a-záéíóú]+)\s+(?:de\s+)?(202[5-7])', t):
        mes = MESES.get(m.group(2)[:3])
        if mes:
            try: return date(int(m.group(3)),mes,int(m.group(1))).isoformat()
            except: pass
    # "April 24-26, 2026" / "April 24, 2026"
    for m in re.finditer(r'([a-z]+)\s+(\d{1,2})(?:-\d+)?,?\s+(202[5-7])', t):
        mes = MESES.get(m.group(1)[:3])
        if mes:
            try: return date(int(m.group(3)),mes,int(m.group(2))).isoformat()
            except: pass
    # "25/05/2026"
    for m in re.finditer(r'(\d{1,2})[/\-](\d{1,2})[/\-](202[5-7])', texto):
        try: return date(int(m.group(3)),int(m.group(2)),int(m.group(1))).isoformat()
        except: pass
    return None

def es_hackathon(titulo):
    t = titulo.lower()
    return any(k in t for k in ["hackathon","hackaton","hackatón","hackfest","datathon","codeathon"])


# ══════════════════════════════════════════════════════════════
# FUENTE 1: DEVPOST JSON
# ══════════════════════════════════════════════════════════════
def scrape_devpost():
    print("\n📡 Fuente 1: Devpost...")
    eventos = []
    urls = [
        "https://devpost.com/hackathons.json?challenge_type=all&status%5B%5D=upcoming&order_by=deadline&per_page=50&location=Spain",
        "https://devpost.com/hackathons.json?challenge_type=all&status%5B%5D=upcoming&order_by=deadline&per_page=50&location=Barcelona",
        "https://devpost.com/hackathons.json?challenge_type=all&status%5B%5D=upcoming&order_by=deadline&per_page=50&location=Madrid",
    ]
    vistos = set()
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=12)
            if r.status_code != 200:
                print(f"   ⚠️ HTTP {r.status_code}")
                continue
            data = r.json()
            for h in data.get("hackathons", []):
                u = h.get("url","")
                if not u or u in vistos: continue
                vistos.add(u)
                nombre    = h.get("title","")
                dates_str = h.get("submission_period_dates","") or ""
                location  = h.get("location","") or ""
                ciudad    = extraer_ciudad(f"{nombre} {location}")
                fecha     = extraer_fecha(dates_str) or extraer_fecha(nombre)
                eventos.append({
                    "nombre":         nombre[:300],
                    "descripcion":    h.get("tagline","")[:1000],
                    "url":            u,
                    "ciudad":         ciudad,
                    "online":         ciudad=="Online",
                    "fecha_inicio":   fecha,
                    "fuente":         "Devpost",
                    "fecha_scraping": datetime.now().isoformat(),
                })
                print(f"   ✓ {nombre[:55]} | {fecha or '?'} | {ciudad or '?'}")
            time.sleep(1)
        except Exception as e:
            print(f"   ⚠️ {e}")
    print(f"   → {len(eventos)} de Devpost")
    return eventos


# ══════════════════════════════════════════════════════════════
# FUENTE 2: HACKATHONSPAIN.COM
# ══════════════════════════════════════════════════════════════
def scrape_hackathonspain():
    print("\n📡 Fuente 2: HackathonSpain.com...")
    eventos = []
    try:
        r = requests.get("https://hackathonspain.com/calendario/", headers=HEADERS, timeout=15)
        html = r.text

        # Buscar enlaces a eventos individuales en el calendario
        # Los eventos tienen URLs tipo /calendario/nombre-hackathon/
        links = re.findall(
            r'href=["\']https?://hackathonspain\.com/calendario/([^"\'/#]+)/["\']',
            html
        )
        links = list(set(links))
        links = [l for l in links if l and l != "calendario"]

        print(f"   → {len(links)} páginas de eventos a visitar")

        for slug in links[:30]:  # límite razonable
            url_ev = f"https://hackathonspain.com/calendario/{slug}/"
            try:
                re2 = requests.get(url_ev, headers=HEADERS, timeout=10)
                html2 = re2.text
                texto_limpio = re.sub(r'<[^>]+>', ' ', html2)
                texto_limpio = re.sub(r'\s+', ' ', texto_limpio)

                # Nombre: primer h1
                nombre_m = re.search(r'<h1[^>]*>(.*?)</h1>', html2, re.DOTALL)
                nombre = re.sub(r'<[^>]+>', '', nombre_m.group(1)).strip() if nombre_m else slug.replace('-', ' ').title()

                if not es_hackathon(nombre):
                    continue

                fecha  = extraer_fecha(texto_limpio)
                ciudad = extraer_ciudad(f"{nombre} {texto_limpio[:2000]}")

                eventos.append({
                    "nombre":         nombre[:300],
                    "descripcion":    texto_limpio[:800],
                    "url":            url_ev,
                    "ciudad":         ciudad,
                    "online":         ciudad=="Online",
                    "fecha_inicio":   fecha,
                    "fuente":         "HackathonSpain",
                    "fecha_scraping": datetime.now().isoformat(),
                })
                print(f"   ✓ {nombre[:55]} | {fecha or '?'} | {ciudad or '?'}")
                time.sleep(0.4)
            except Exception as e:
                print(f"   ⚠️ {slug}: {e}")

    except Exception as e:
        print(f"   ⚠️ Error general: {e}")

    print(f"   → {len(eventos)} de HackathonSpain")
    return eventos


# ══════════════════════════════════════════════════════════════
# FUENTE 3: DUCKDUCKGO (filtrado estricto)
# ══════════════════════════════════════════════════════════════
QUERIES_DDG = [
    "hackathon España 2026 convocatoria",
    "hackathon Madrid 2026",
    "hackathon Barcelona 2026",
    "hackathon Valencia Sevilla Bilbao 2026",
    "hackathon Málaga Granada 2026",
    "hackathon universitario España 2026",
    "hackathon inteligencia artificial España 2026",
    "NASA hackathon España 2026",
    "hackathon startups España 2026",
]

def scrape_duckduckgo():
    print("\n📡 Fuente 3: DuckDuckGo (filtrado estricto)...")
    eventos = []
    urls_vistas = set()

    with DDGS() as ddgs:
        for query in QUERIES_DDG:
            try:
                hits = ddgs.text(query, max_results=10)
                for r in hits:
                    url   = r.get("href","")
                    titulo = r.get("title","")
                    cuerpo = r.get("body","")
                    texto  = f"{titulo} {cuerpo} {url}"

                    # Filtros de exclusión
                    if not url or url in urls_vistas:          continue
                    if not es_url_valida(url):                 continue
                    if not es_hackathon(titulo):               continue
                    if not es_de_espana(texto):                continue

                    urls_vistas.add(url)
                    ciudad = extraer_ciudad(texto)
                    fecha  = extraer_fecha(f"{titulo} {cuerpo}")

                    # Buscar fecha en la página si no la tenemos
                    if not fecha and ".es" in url:
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
# GUARDAR EN SUPABASE
# ══════════════════════════════════════════════════════════════
def guardar(eventos):
    nuevos = actualizados = 0
    for ev in eventos:
        if not ev.get("nombre") or not ev.get("url"): continue
        try:
            ex = supabase.table("hackathons")\
                .select("id,ciudad,fecha_inicio")\
                .eq("url", ev["url"]).execute()
            if not ex.data:
                supabase.table("hackathons").insert(ev).execute()
                nuevos += 1
                print(f"   ✅ {ev['nombre'][:65]}")
            else:
                row = ex.data[0]
                upd = {}
                if ev.get("ciudad")      and not row.get("ciudad"):      upd["ciudad"]      = ev["ciudad"]
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
    print("🚀 hackathons.es scraper v4")
    todos  = scrape_devpost()
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
