"""
hackathons.es — Scraper v10 — Enfoque honesto y fiable
Qué hace:
  1. Limpia la BD (elimina basura)
  2. Actualiza fechas faltantes via JSON-LD en cada web
  3. Lista curada de hackathons españoles conocidos (específicos)
  4. DuckDuckGo como complemento, visitando cada URL para JSON-LD
  5. Deduplicación por URL + similitud título
"""
import os, re, json, time, difflib, requests
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
    "ene":1,"enero":1,"feb":2,"febrero":2,"mar":3,"marzo":3,"abr":4,"abril":4,
    "may":5,"mayo":5,"jun":6,"junio":6,"jul":7,"julio":7,"ago":8,"agosto":8,
    "sep":9,"septiembre":9,"oct":10,"octubre":10,"nov":11,"noviembre":11,"dic":12,"diciembre":12,
    "jan":1,"january":1,"february":2,"march":3,"april":4,"june":6,"july":7,
    "august":8,"september":9,"october":10,"november":11,"december":12,
}

BASURA = [
    # Redes sociales y vídeos
    "youtube.com","youtu.be","instagram.com","tiktok.com","twitter.com",
    "x.com","facebook.com","vk.com","rutube.ru","linkedin.com","reddit.com",
    # Contenido no español o irrelevante
    "смотреть","сериал","серия","фильм","онлайн","viral","terbaru",
    "mackolik","takım","kinogo","rosserialls","pinkviral","kaz-media",
    "futbol","fútbol","football","ecuador","colombia","scrum",
    # Páginas de listado / CMS (no son eventos reales)
    "upcoming hackathons in","find hackathon","find & organize",
    "publica tu hackathon","quieres organizar","guía completa",
    "list of hackathons","hackathons events","hackathon events",
    "things to do","discover hackathon","search hackathon",
    "wikipedia.org","medium.com","/tag/","#hackathon",
    # Noticias antiguas / recaps
    "recap:","ganadores del","los ganadores","celebrouse","celebrado en",
    "celebramos","celebrado el","ya hemos","así fue",
]

PAISES_EXCLUIDOS = [
    "helsinki","finland","amsterdam","netherlands"," london "," paris ",
    " berlin ","new york","san francisco","toronto","ecuador","colombia",
    "mexico ","argentina","peru","chile","brasil","brazil","kenya",
    "india ","china ","japan","korea","australia","austria","poland",
    "italy ","ukraine","turkey","sweden","norway","denmark","aalto",
]

def ciudad_de(txt):
    t = txt.lower()
    if any(w in t for w in ["online","virtual","remoto","remote","worldwide"]): return "Online"
    for c in CIUDADES:
        if c.lower() in t: return c
    return None

def es_hackathon(titulo):
    t = titulo.lower()
    return any(k in t for k in ["hackathon","hackaton","hackatón","hackfest","datathon","hack the","hack for"])

def es_de_espana(txt):
    t = txt.lower()
    if any(p in t for p in PAISES_EXCLUIDOS): return False
    ok = ["españa","spain","madrid","barcelona","valencia","sevilla","bilbao",
          "málaga","malaga","zaragoza","granada","murcia","alicante",".es/",
          "español","española","universit","hackathonspain","nasaspaceapps"]
    return any(w in t for w in ok)

def es_basura(nombre, url=""):
    txt = (nombre + " " + url).lower()
    return any(b in txt for b in BASURA)

def limpiar_html(html): return re.sub(r'\s+',' ', re.sub(r'<[^>]+>',' ',html)).strip()

def extraer_fecha(txt):
    if not txt: return None
    t = str(txt)
    # ISO
    for m in re.finditer(r'(202[5-7])-(\d{2})-(\d{2})', t):
        try: return date(int(m.group(1)),int(m.group(2)),int(m.group(3))).isoformat()
        except: pass
    tl = t.lower()
    # "24 de mayo de 2026"
    for m in re.finditer(r'(\d{1,2})\s+(?:de\s+)?([a-záéíóú]{3,})\s+(?:de\s+)?(202[5-7])', tl):
        mes = MESES.get(m.group(2)[:3])
        if mes:
            try: return date(int(m.group(3)),mes,int(m.group(1))).isoformat()
            except: pass
    # "April 24-26, 2026"
    for m in re.finditer(r'([a-z]{3,})\s+(\d{1,2})(?:\s*[-–]\s*\d+)?,?\s+(202[5-7])', tl):
        mes = MESES.get(m.group(1)[:3])
        if mes:
            try: return date(int(m.group(3)),mes,int(m.group(2))).isoformat()
            except: pass
    # "24/05/2026"
    for m in re.finditer(r'(\d{1,2})[/\-](\d{1,2})[/\-](202[5-7])', t):
        try: return date(int(m.group(3)),int(m.group(2)),int(m.group(1))).isoformat()
        except: pass
    return None

def fecha_de_html(html):
    """JSON-LD > meta > texto. El más fiable."""
    if not html: return None
    # 1. JSON-LD Schema.org Event
    for s in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL|re.IGNORECASE):
        try:
            data = json.loads(s.group(1).strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if "@graph" in item: items += item["@graph"]
                if any(t in str(item.get("@type","")) for t in ("Event","Hackathon","Education","Course")):
                    for campo in ("startDate","startTime","datePublished"):
                        val = item.get(campo,"")
                        if val:
                            f = extraer_fecha(str(val)[:25])
                            if f: return f
        except: pass
    # 2. meta tags
    for pat in [
        r'<meta[^>]+property=["\']event:start_time["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']date["\'][^>]+content=["\']([^"\']+)["\']',
        r'<time[^>]+datetime=["\']([^"\']+)["\']',
        r'startDate["\s:]+["\']([^"\']{8,25})["\']',
        r'"start_at"\s*:\s*"([^"]{10,25})"',
        r'"starts_at"\s*:\s*"([^"]{10,25})"',
    ]:
        for m in re.finditer(pat, html, re.IGNORECASE):
            f = extraer_fecha(m.group(1))
            if f: return f
    # 3. Texto plano
    return extraer_fecha(limpiar_html(html[:60000]))

def fetch(url, timeout=10):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.text if r.status_code == 200 else None
    except: return None


# ═══════════════════════════════════════════════════════════════
# PASO 1: LIMPIAR BD
# ═══════════════════════════════════════════════════════════════
def limpiar_bd():
    print("\n🧹 Limpiando BD de basura...")
    try:
        rows = supabase.table("hackathons").select("id,nombre,url").execute()
        borrados = 0
        for row in rows.data or []:
            if not es_hackathon(row.get("nombre","")) or es_basura(row.get("nombre",""), row.get("url","")):
                supabase.table("hackathons").delete().eq("id",row["id"]).execute()
                borrados += 1
                print(f"   🗑️  {row['nombre'][:65]}")
        print(f"   → {borrados} eliminadas")
    except Exception as e:
        print(f"   ⚠️ {e}")


# ═══════════════════════════════════════════════════════════════
# PASO 2: ACTUALIZAR SIN FECHA (JSON-LD de cada web)
# ═══════════════════════════════════════════════════════════════
def actualizar_sin_fecha():
    print("\n🔄 Actualizando sin fecha via JSON-LD...")
    try:
        rows = supabase.table("hackathons")\
            .select("id,nombre,url,ciudad")\
            .is_("fecha_inicio","null")\
            .not_.is_("url","null")\
            .limit(100).execute()
        actualizados = 0
        for row in rows.data or []:
            url = row.get("url","")
            if not url or not url.startswith("http"): continue
            html = fetch(url, timeout=8)
            if not html: continue
            fecha  = fecha_de_html(html)
            ciudad = row.get("ciudad") or ciudad_de(limpiar_html(html[:5000]))
            upd = {}
            if fecha:  upd["fecha_inicio"] = fecha
            if ciudad and not row.get("ciudad"): upd["ciudad"] = ciudad
            if upd:
                supabase.table("hackathons").update(upd).eq("id",row["id"]).execute()
                if fecha: actualizados += 1
                print(f"   🔄 {row['nombre'][:60]} → {fecha or '(ciudad)'}")
            time.sleep(0.35)
        print(f"   → {actualizados} con fecha nueva")
    except Exception as e:
        print(f"   ⚠️ {e}")


# ═══════════════════════════════════════════════════════════════
# PASO 3: LISTA CURADA — hackathons españoles conocidos
# Estas URLs se actualizan manualmente cada mes
# ═══════════════════════════════════════════════════════════════
EVENTOS_CURADOS = [
    # 2026 — confirmados
    ("https://hackupc.com", "HackUPC 2026", "2026-04-24", "Barcelona"),
    ("https://hackspain.com", "HACKSPAIN 2026", "2026-06-01", "Madrid"),
    ("https://hackudc.vercel.app", "HackUDC 2026", "2026-03-21", "A Coruña"),
    ("https://junction.hackathon.com", "Junction Hackathon 2026", None, None),
    ("https://spaceappschallenge.org", "NASA Space Apps Challenge España 2026", "2026-10-04", "España"),
    # Añadir aquí nuevos eventos confirmados cuando se conozcan
]

def procesar_curados():
    print("\n📋 Procesando lista curada...")
    eventos = []
    for url, nombre, fecha, ciudad in EVENTOS_CURADOS:
        # Intentar obtener más datos de la web
        html = fetch(url)
        if html:
            fecha_web = fecha_de_html(html)
            ciudad_web = ciudad_de(limpiar_html(html[:5000]))
            fecha  = fecha  or fecha_web
            ciudad = ciudad or ciudad_web
            # Nombre real de la web
            h1 = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL|re.IGNORECASE)
            if h1:
                nombre_web = limpiar_html(h1.group(1))
                if es_hackathon(nombre_web) and len(nombre_web) > 5:
                    nombre = nombre_web
        eventos.append({
            "nombre": nombre[:300],
            "descripcion": "",
            "url": url,
            "ciudad": ciudad,
            "online": ciudad == "Online",
            "fecha_inicio": fecha,
            "fuente": "Curado",
            "fecha_scraping": datetime.now().isoformat(),
        })
        print(f"   ✓ {nombre[:60]} | {fecha or '?'} | {ciudad or '?'}")
        time.sleep(0.4)
    return eventos


# ═══════════════════════════════════════════════════════════════
# PASO 4: DUCKDUCKGO — filtrado + JSON-LD en cada página
# ═══════════════════════════════════════════════════════════════
QUERIES_DDG = [
    "hackathon España 2026 convocatoria inscripción",
    "hackathon Madrid 2026",
    "hackathon Barcelona 2026",
    "hackathon Valencia Sevilla Bilbao 2026",
    "hackathon Málaga Granada Zaragoza 2026",
    "hackathon universitario España 2026",
    "hackathon inteligencia artificial España 2026",
    "NASA Space Apps hackathon España 2026",
]

DOMINIOS_BLOQUEADOS = [
    "youtube.com","youtu.be","instagram.com","tiktok.com","twitter.com","x.com",
    "facebook.com","vk.com","rutube.ru","wikipedia.org","reddit.com","linkedin.com",
    "pinkviral","mackolik","kinogo","latroupe","kaz-media","scrum",
]

def scrape_ddg():
    print("\n🦆 DuckDuckGo + JSON-LD en cada página...")
    eventos = []
    vistos = set()

    with DDGS() as ddgs:
        for query in QUERIES_DDG:
            try:
                hits = ddgs.text(query, max_results=8)
                for r in hits:
                    url    = r.get("href","")
                    titulo = r.get("title","")
                    cuerpo = r.get("body","")
                    texto  = f"{titulo} {cuerpo} {url}"

                    if not url or url in vistos: continue
                    if any(d in url.lower() for d in DOMINIOS_BLOQUEADOS): continue
                    if not es_hackathon(titulo): continue
                    if not es_de_espana(texto): continue
                    # Filtrar páginas de listado / no-eventos
                    titulo_lower = titulo.lower()
                    if any(b in titulo_lower for b in [
                        "upcoming hackathons in","find hackathons","find & organize",
                        "list of hackathons","discover hackathon","hackathon events in",
                        "things to do","publica tu","quieres organizar","guía completa",
                        "recap:","ganadores","celebrouse","así fue el",
                    ]): continue
                    vistos.add(url)

                    # Visitar la página para JSON-LD
                    html = fetch(url, timeout=8)
                    fecha  = fecha_de_html(html) if html else extraer_fecha(f"{titulo} {cuerpo}")
                    ciudad = ciudad_de(texto)
                    if html and not ciudad:
                        ciudad = ciudad_de(limpiar_html(html[:5000]))

                    eventos.append({
                        "nombre": titulo[:300],
                        "descripcion": cuerpo[:1000],
                        "url": url,
                        "ciudad": ciudad,
                        "online": ciudad=="Online",
                        "fecha_inicio": fecha,
                        "fuente": "DuckDuckGo",
                        "fecha_scraping": datetime.now().isoformat(),
                    })
                    print(f"   ✓ {titulo[:55]} | {fecha or '?'} | {ciudad or '?'}")
                    time.sleep(0.35)
            except Exception as e:
                print(f"   ⚠️ '{query}': {e}")

    print(f"   → {len(eventos)} de DuckDuckGo")
    return eventos


# ═══════════════════════════════════════════════════════════════
# DEDUPLICACIÓN
# ═══════════════════════════════════════════════════════════════
def norm_url(url): return re.sub(r'[/?#]$','', url.lower().split('?')[0])

def son_dup(a, b):
    if norm_url(a["url"]) == norm_url(b["url"]): return True
    return difflib.SequenceMatcher(None, a["nombre"].lower(), b["nombre"].lower()).ratio() > 0.88

def dedup(eventos):
    unicos = []
    for ev in eventos:
        dup = False
        for i, ex in enumerate(unicos):
            if son_dup(ev, ex):
                if ev.get("fecha_inicio") and not ex.get("fecha_inicio"):
                    unicos[i] = ev
                dup = True; break
        if not dup: unicos.append(ev)
    return unicos


# ═══════════════════════════════════════════════════════════════
# GUARDAR
# ═══════════════════════════════════════════════════════════════
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


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 hackathons.es scraper v10")

    limpiar_bd()
    actualizar_sin_fecha()

    todos  = procesar_curados()
    todos += scrape_ddg()
    todos  = dedup(todos)

    print(f"\n📦 Total únicos: {len(todos)}")
    guardar(todos)
    print("\n✅ Listo")
