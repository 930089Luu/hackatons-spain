import os
import re
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
}

CIUDADES = [
    "Madrid","Barcelona","Valencia","Sevilla","Bilbao","Málaga","Zaragoza",
    "Murcia","Palma","Las Palmas","Alicante","Córdoba","Valladolid","Vigo",
    "Gijón","Granada","Pamplona","Salamanca","San Sebastián","Santander",
    "Toledo","Burgos","Oviedo","Albacete","Teruel","Cádiz","Huelva","Almería",
    "Donostia","Logroño","Castellón","Badajoz","Lleida","Tarragona","Girona",
    "Huesca","Lugo","Pontevedra","Ourense","A Coruña","Gandia","Castellón",
    "Elche","Cartagena","Jerez","Alcobendas","Getafe",
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
    "pinkviral","mackolik","kinogo","latroupe","kaz-media","scrum",
]

PAISES_EXCLUIDOS = [
    "helsinki","finland","amsterdam","netherlands"," london ","paris ",
    " berlin ","new york","san francisco","toronto","ecuador","colombia",
    "mexico ","argentina","peru","chile","brasil","brazil","kenya",
    "india ","china ","japan","korea","australia","austria","poland",
    "italy ","ukraine","turkey","sweden","norway","denmark",
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
    """Extractor inteligente: primero busca año, luego mes+día cerca."""
    if not texto: return None
    t = texto.lower()

    # 1. ISO directo: 2026-05-24
    for m in re.finditer(r'(202[5-7])-(\d{2})-(\d{2})', texto):
        try: return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
        except: pass

    # 2. "25 de mayo de 2026" / "25 mayo 2026"
    for m in re.finditer(r'(\d{1,2})\s+(?:de\s+)?([a-záéíóú]{3,})\s+(?:de\s+)?(202[5-7])', t):
        mes = MESES.get(m.group(2)[:3])
        if mes:
            try: return date(int(m.group(3)), mes, int(m.group(1))).isoformat()
            except: pass

    # 3. "April 24, 2026" / "April 24-26, 2026"
    for m in re.finditer(r'([a-z]{3,})\s+(\d{1,2})(?:\s*[-–]\s*\d+)?,?\s+(202[5-7])', t):
        mes = MESES.get(m.group(1)[:3])
        if mes:
            try: return date(int(m.group(3)), mes, int(m.group(2))).isoformat()
            except: pass

    # 4. "25/04/2026" o "25-04-2026"
    for m in re.finditer(r'(\d{1,2})[/\-](\d{1,2})[/\-](202[5-7])', texto):
        try: return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
        except: pass

    # 5. NUEVO: año + mes en cualquier orden dentro de ~50 chars
    #    "2026 | April 24" / "2026... abril... 25"
    years = [int(m.group()) for m in re.finditer(r'202[5-7]', texto)]
    for year in set(years):
        for mes_str, mes_num in MESES.items():
            if len(mes_str) < 3: continue
            idx = t.find(mes_str)
            while idx != -1:
                # Buscar día cerca (±60 chars)
                contexto = t[max(0, idx-5):idx+len(mes_str)+10]
                dm = re.search(r'(\d{1,2})', contexto.replace(str(year),''))
                if dm:
                    dia = int(dm.group(1))
                    if 1 <= dia <= 31:
                        try:
                            return date(year, mes_num, dia).isoformat()
                        except: pass
                idx = t.find(mes_str, idx+1)

    return None

def es_hackathon(titulo):
    t = titulo.lower()
    return any(k in t for k in ["hackathon","hackaton","hackatón","hackfest","datathon","hack the","hack for"])

def limpiar_html(html):
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', html)).strip()

def fetch_fecha(url):
    """Visita la URL y extrae la fecha del HTML."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        if r.status_code == 200:
            return extraer_fecha(r.text[:60000])
    except: pass
    return None


# ══════════════════════════════════════════════════════════════
# FUENTE 1: HACKATHON.COM/COUNTRY/SPAIN
# ══════════════════════════════════════════════════════════════
def scrape_hackathon_com():
    print("\n📡 Fuente 1: hackathon.com/country/spain...")
    eventos = []
    try:
        r = requests.get("https://www.hackathon.com/country/spain/2026",
                         headers=HEADERS, timeout=15)
        html = r.text

        # Extraer bloques de eventos: buscar links a /event/
        event_paths = list(set(re.findall(r'/event/([^"\'?\s]+)', html)))
        print(f"   → {len(event_paths)} eventos encontrados")

        for path in event_paths[:30]:
            url_ev = f"https://www.hackathon.com/event/{path}"
            try:
                r2 = requests.get(url_ev, headers=HEADERS, timeout=10)
                if r2.status_code != 200: continue
                html2 = r2.text
                texto = limpiar_html(html2)

                h1 = re.search(r'<h1[^>]*>(.*?)</h1>', html2, re.DOTALL|re.IGNORECASE)
                nombre = limpiar_html(h1.group(1)) if h1 else path.replace('-',' ').title()
                if not nombre or not es_hackathon(nombre): continue
                if not es_de_espana(f"{nombre} {texto[:2000]}"): continue

                fecha  = extraer_fecha(texto[:8000])
                ciudad = extraer_ciudad(f"{nombre} {texto[:3000]}")

                eventos.append({
                    "nombre": nombre[:300], "descripcion": texto[:800],
                    "url": url_ev, "ciudad": ciudad,
                    "online": ciudad == "Online", "fecha_inicio": fecha,
                    "fuente": "Hackathon.com",
                    "fecha_scraping": datetime.now().isoformat(),
                })
                print(f"   ✓ {nombre[:55]} | {fecha or '?'} | {ciudad or '?'}")
                time.sleep(0.5)
            except: pass

    except Exception as e:
        print(f"   ⚠️ {e}")

    print(f"   → {len(eventos)} de hackathon.com")
    return eventos


# ══════════════════════════════════════════════════════════════
# FUENTE 2: DUCKDUCKGO
# ══════════════════════════════════════════════════════════════
QUERIES = [
    "hackathon España 2026 fecha inscripción",
    "hackathon Madrid 2026",
    "hackathon Barcelona 2026",
    "hackathon Valencia Sevilla Bilbao 2026",
    "hackathon Málaga Granada Zaragoza 2026",
    "hackathon universitario España 2026",
    "hackathon inteligencia artificial España 2026",
    "NASA Space Apps hackathon España 2026",
    "convocatoria hackathon España 2026 premio",
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
                    fecha  = extraer_fecha(f"{titulo} {cuerpo}")

                    # Siempre intentar la página si no tenemos fecha
                    if not fecha:
                        fecha = fetch_fecha(url)
                        time.sleep(0.3)

                    eventos.append({
                        "nombre": titulo[:300], "descripcion": cuerpo[:1000],
                        "url": url, "ciudad": ciudad,
                        "online": ciudad == "Online", "fecha_inicio": fecha,
                        "fuente": "DuckDuckGo",
                        "fecha_scraping": datetime.now().isoformat(),
                    })
                    estado = fecha or '?'
                    print(f"   ✓ {titulo[:52]} | {estado} | {ciudad or '?'}")

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
    print("🚀 hackathons.es scraper v7")
    todos  = scrape_hackathon_com()
    todos += scrape_duckduckgo()

    vistos, unicos = set(), []
    for ev in todos:
        if ev["url"] not in vistos:
            vistos.add(ev["url"])
            unicos.append(ev)

    print(f"\n📦 Total únicos: {len(unicos)}")
    guardar(unicos)
    print("\n✅ Listo")
