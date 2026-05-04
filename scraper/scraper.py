"""
hackathons.es — Scraper definitivo v9
Estrategia:
  1. LIMPIA la BD (elimina basura: series rusas, fútbol, etc.)
  2. Fuente A: Luma city pages (__NEXT_DATA__ JSON, fechas perfectas)
  3. Fuente B: Lista curada de URLs españolas con JSON-LD
  4. Deduplicación por URL + similitud de título
"""
import os, re, json, time, difflib, requests
from datetime import datetime, date
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

# ────────────────────────────────────────────────────────────────
# CIUDADES
# ────────────────────────────────────────────────────────────────
CIUDADES = [
    "Madrid","Barcelona","Valencia","Sevilla","Bilbao","Málaga","Zaragoza",
    "Murcia","Palma","Las Palmas","Alicante","Córdoba","Valladolid","Vigo",
    "Gijón","Granada","Pamplona","Salamanca","San Sebastián","Santander",
    "Toledo","Burgos","Oviedo","Albacete","Teruel","Cádiz","Huelva","Almería",
    "Donostia","Logroño","Castellón","Badajoz","Lleida","Tarragona","Girona",
    "Huesca","Lugo","Pontevedra","Ourense","A Coruña","Gandia","Elche",
]

def ciudad_de(texto):
    t = texto.lower()
    if any(w in t for w in ["online","virtual","remoto","remote","worldwide"]): return "Online"
    for c in CIUDADES:
        if c.lower() in t: return c
    return None

def es_hackathon(titulo):
    t = titulo.lower()
    return any(k in t for k in ["hackathon","hackaton","hackatón","hackfest","datathon","hack the","hack for"])

def limpiar(html):
    return re.sub(r'\s+',' ', re.sub(r'<[^>]+>',' ', html)).strip()

# ────────────────────────────────────────────────────────────────
# PASO 1: LIMPIAR BD (quitar basura)
# ────────────────────────────────────────────────────────────────
PALABRAS_BASURA = [
    "смотреть","сериал","серия","фильм","онлайн","viral","terbaru",
    "futbol","fútbol","football","mackolik","takım","youtube.com",
    "instagram.com","tiktok.com","vk.com","rutube.ru","kinogo",
    "rosserialls","pinkviral","mackolik","kaz-media","latroupe",
]

def limpiar_bd():
    print("\n🧹 Paso 1: Limpiando BD...")
    try:
        rows = supabase.table("hackathons").select("id,nombre,url").execute()
        borrados = 0
        for row in rows.data or []:
            nombre = (row.get("nombre") or "").lower()
            url    = (row.get("url")    or "").lower()
            texto  = nombre + " " + url
            # Eliminar si no es hackathon o contiene basura
            if not es_hackathon(row.get("nombre","")) or any(b in texto for b in PALABRAS_BASURA):
                supabase.table("hackathons").delete().eq("id", row["id"]).execute()
                borrados += 1
                print(f"   🗑️  {row['nombre'][:60]}")
        print(f"   → {borrados} entradas basura eliminadas")
    except Exception as e:
        print(f"   ⚠️ Error limpiando: {e}")

# ────────────────────────────────────────────────────────────────
# EXTRAER FECHA
# ────────────────────────────────────────────────────────────────
MESES = {
    "ene":1,"enero":1,"feb":2,"febrero":2,"mar":3,"marzo":3,"abr":4,"abril":4,
    "may":5,"mayo":5,"jun":6,"junio":6,"jul":7,"julio":7,"ago":8,"agosto":8,
    "sep":9,"septiembre":9,"oct":10,"octubre":10,"nov":11,"noviembre":11,"dic":12,"diciembre":12,
    "jan":1,"january":1,"february":2,"march":3,"april":4,"june":6,"july":7,
    "august":8,"september":9,"october":10,"november":11,"december":12,
}

def extraer_fecha(txt):
    if not txt: return None
    t = str(txt).lower()
    # ISO 2026-05-24T...
    for m in re.finditer(r'(202[5-7])-(\d{2})-(\d{2})', txt):
        try: return date(int(m.group(1)),int(m.group(2)),int(m.group(3))).isoformat()
        except: pass
    # "24 de mayo de 2026"
    for m in re.finditer(r'(\d{1,2})\s+(?:de\s+)?([a-záéíóú]{3,})\s+(?:de\s+)?(202[5-7])', t):
        mes = MESES.get(m.group(2)[:3])
        if mes:
            try: return date(int(m.group(3)),mes,int(m.group(1))).isoformat()
            except: pass
    # "April 24-26, 2026" / "April 24, 2026"
    for m in re.finditer(r'([a-z]{3,})\s+(\d{1,2})(?:\s*[-–]\s*\d+)?,?\s+(202[5-7])', t):
        mes = MESES.get(m.group(1)[:3])
        if mes:
            try: return date(int(m.group(3)),mes,int(m.group(2))).isoformat()
            except: pass
    # "24/05/2026"
    for m in re.finditer(r'(\d{1,2})[/\-](\d{1,2})[/\-](202[5-7])', txt):
        try: return date(int(m.group(3)),int(m.group(2)),int(m.group(1))).isoformat()
        except: pass
    return None

# ────────────────────────────────────────────────────────────────
# EXTRAER FECHA DE HTML (JSON-LD > meta > text)
# ────────────────────────────────────────────────────────────────
def fecha_de_html(html):
    # 1. JSON-LD Schema.org Event
    for script in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL|re.IGNORECASE):
        try:
            data = json.loads(script.group(1).strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if "@graph" in item: items += item["@graph"]
                tipo = str(item.get("@type",""))
                if any(t in tipo for t in ("Event","Hackathon","Education","Course","Social")):
                    for campo in ("startDate","datePublished","startTime","dateCreated"):
                        val = item.get(campo,"")
                        if val:
                            f = extraer_fecha(str(val)[:25])
                            if f: return f
        except: pass

    # 2. __NEXT_DATA__ (Luma, Next.js)
    nd = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html, re.DOTALL)
    if nd:
        try:
            data = json.loads(nd.group(1))
            text = json.dumps(data)
            f = extraer_fecha(text[:20000])
            if f: return f
        except: pass

    # 3. meta tags
    for pat in [
        r'<meta[^>]+(?:property|name)=["\'][^"\']*(?:start|event)[^"\']*["\'][^>]+content=["\']([^"\']+)["\']',
        r'<time[^>]+datetime=["\']([^"\']+)["\']',
        r'startDate["\s:]+["\']([^"\']{8,25})["\']',
    ]:
        for m in re.finditer(pat, html, re.IGNORECASE):
            f = extraer_fecha(m.group(1))
            if f: return f

    # 4. Texto plano como último recurso
    return extraer_fecha(limpiar(html[:60000]))

def fetch(url, timeout=10):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.text if r.status_code == 200 else None
    except: return None

# ────────────────────────────────────────────────────────────────
# FUENTE A: LUMA CITY PAGES
# Luma embebe todos sus eventos en __NEXT_DATA__ con startAt ISO
# ────────────────────────────────────────────────────────────────
LUMA_CITIES = ["madrid","barcelona","valencia","sevilla","bilbao","malaga","zaragoza","granada"]

def scrape_luma():
    print("\n📡 Fuente A: Luma city pages...")
    eventos = []
    for city in LUMA_CITIES:
        url = f"https://lu.ma/{city}"
        html = fetch(url)
        if not html:
            print(f"   ⚠️ No se pudo cargar {url}")
            continue
        try:
            nd = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html, re.DOTALL)
            if not nd: continue
            data = json.loads(nd.group(1))
            # Buscar eventos en el JSON
            text = json.dumps(data)
            # Extraer bloques de eventos: buscar objetos con "start_at" y "name"
            event_blocks = []
            # Luma guarda eventos en initialData.events o similar
            def buscar_eventos(obj, depth=0):
                if depth > 12: return
                if isinstance(obj, dict):
                    # Si tiene start_at y name es un evento
                    if "start_at" in obj and ("name" in obj or "title" in obj):
                        event_blocks.append(obj)
                    else:
                        for v in obj.values():
                            buscar_eventos(v, depth+1)
                elif isinstance(obj, list):
                    for item in obj:
                        buscar_eventos(item, depth+1)
            buscar_eventos(data)

            for ev in event_blocks:
                nombre = ev.get("name") or ev.get("title") or ""
                if not nombre or not es_hackathon(nombre): continue
                start_at = ev.get("start_at","")
                fecha = extraer_fecha(start_at[:25]) if start_at else None
                url_ev = ev.get("url") or ev.get("event_url") or ""
                if url_ev and not url_ev.startswith("http"):
                    url_ev = f"https://lu.ma/{url_ev}"
                if not url_ev: continue
                ciudad_ev = ciudad_de(f"{nombre} {city} {ev.get('geo_address_info',{})}")
                eventos.append({
                    "nombre": nombre[:300],
                    "descripcion": (ev.get("description") or "")[:1000],
                    "url": url_ev,
                    "ciudad": ciudad_ev,
                    "online": ciudad_ev == "Online",
                    "fecha_inicio": fecha,
                    "fuente": "Luma",
                    "fecha_scraping": datetime.now().isoformat(),
                })
                print(f"   ✓ {nombre[:55]} | {fecha or '?'} | {ciudad_ev or '?'}")
        except Exception as e:
            print(f"   ⚠️ {city}: {e}")
        time.sleep(0.5)
    print(f"   → {len(eventos)} de Luma")
    return eventos

# ────────────────────────────────────────────────────────────────
# FUENTE B: LISTA CURADA de sitios españoles con JSON-LD
# ────────────────────────────────────────────────────────────────
CURATED_URLS = [
    # Directorios
    "https://hackathonspain.com/calendario/",
    "https://www.hackathon.com/country/spain/2026",
    # Hackathons conocidos con JSON-LD
    "https://hackupc.com/",
    "https://hackspain.com/",
    "https://junction.hackathon.com/",
    "https://www.eventbrite.es/d/spain/hackathon/",
    "https://www.eventbrite.es/d/spain--madrid/hackathon/",
    "https://www.eventbrite.es/d/spain--barcelona/hackathon/",
    "https://www.eventbrite.es/d/spain--valencia/hackathon/",
    "https://www.meetup.com/find/?keywords=hackathon&location=es--Madrid",
    "https://www.meetup.com/find/?keywords=hackathon&location=es--Barcelona",
    # Eventos conocidos directos
    "https://hackupc.com",
    "https://hackspain.com",
    "https://www.hfull.com",
    "https://www.hackathon.com/city/spain/madrid",
    "https://www.hackathon.com/city/spain/barcelona",
    "https://www.hackathon.com/city/spain/valencia",
    "https://www.hackathon.com/city/spain/seville",
    "https://www.hackathon.com/city/spain/bilbao",
    "https://www.hackathon.com/city/spain/malaga",
]

def scrape_curated():
    print("\n📡 Fuente B: Sitios curados con JSON-LD...")
    eventos = []
    vistos_url = set()

    for base_url in CURATED_URLS:
        html = fetch(base_url)
        if not html: continue

        # Buscar links a eventos individuales en la página
        links = []
        # Eventbrite
        for m in re.finditer(r'href=["\'](https://www\.eventbrite\.es/e/[^"\'?\s]+)', html):
            links.append(m.group(1))
        # hackathon.com events
        for m in re.finditer(r'href=["\'](/event/[^"\'?\s]+)', html):
            links.append("https://www.hackathon.com" + m.group(1))
        # hackathonspain
        for m in re.finditer(r'href=["\'](https://hackathonspain\.com/[^"\'?\s]+)', html):
            links.append(m.group(1))
        # luma
        for m in re.finditer(r'href=["\'](https://lu\.ma/[^"\'?\s]{4,})', html):
            links.append(m.group(1))

        # También intentar extraer directamente de la página base
        links.append(base_url)
        links = list(set(links))

        for url_ev in links[:25]:
            if url_ev in vistos_url: continue
            vistos_url.add(url_ev)

            page_html = fetch(url_ev) if url_ev != base_url else html
            if not page_html: continue

            # Intentar JSON-LD primero
            nombre = ""
            for script in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', page_html, re.DOTALL|re.IGNORECASE):
                try:
                    data = json.loads(script.group(1).strip())
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if "@graph" in item: items += item["@graph"]
                        tipo = str(item.get("@type",""))
                        if any(t in tipo for t in ("Event","Hackathon")):
                            nombre = item.get("name","") or item.get("headline","")
                            fecha  = extraer_fecha(str(item.get("startDate","")))
                            loc    = item.get("location",{})
                            ciudad_txt = (str(loc.get("address","")) + " " + str(loc.get("name","")) if isinstance(loc,dict) else str(loc))
                            ciudad = ciudad_de(f"{nombre} {ciudad_txt}")
                            if nombre and es_hackathon(nombre):
                                eventos.append({
                                    "nombre": nombre[:300],
                                    "descripcion": (item.get("description","") or "")[:1000],
                                    "url": url_ev,
                                    "ciudad": ciudad,
                                    "online": ciudad=="Online",
                                    "fecha_inicio": fecha,
                                    "fuente": "Curated",
                                    "fecha_scraping": datetime.now().isoformat(),
                                })
                                print(f"   ✓ {nombre[:55]} | {fecha or '?'} | {ciudad or '?'}")
                except: pass

            if not nombre:
                # Sin JSON-LD — extraer título y fecha del HTML
                h1 = re.search(r'<h1[^>]*>(.*?)</h1>', page_html, re.DOTALL|re.IGNORECASE)
                nombre = limpiar(h1.group(1)) if h1 else ""
                if nombre and es_hackathon(nombre):
                    fecha  = fecha_de_html(page_html)
                    ciudad = ciudad_de(f"{nombre} {url_ev} {limpiar(page_html[:3000])}")
                    eventos.append({
                        "nombre": nombre[:300],
                        "descripcion": limpiar(page_html[:800]),
                        "url": url_ev,
                        "ciudad": ciudad,
                        "online": ciudad=="Online",
                        "fecha_inicio": fecha,
                        "fuente": "Curated",
                        "fecha_scraping": datetime.now().isoformat(),
                    })
                    print(f"   ✓ {nombre[:55]} | {fecha or '?'} | {ciudad or '?'}")
            time.sleep(0.4)

    print(f"   → {len(eventos)} de sitios curados")
    return eventos

# ────────────────────────────────────────────────────────────────
# PASO 2: ACTUALIZAR REGISTROS SIN FECHA (JSON-LD en su propia web)
# ────────────────────────────────────────────────────────────────
def actualizar_sin_fecha():
    print("\n🔄 Paso 2: Actualizando registros sin fecha...")
    try:
        rows = supabase.table("hackathons")\
            .select("id,nombre,url,ciudad")\
            .is_("fecha_inicio","null")\
            .not_.is_("url","null")\
            .limit(80).execute()
        actualizados = 0
        for row in rows.data or []:
            url = row.get("url","")
            if not url or not url.startswith("http"): continue
            html = fetch(url, timeout=8)
            if not html: continue
            fecha  = fecha_de_html(html)
            ciudad = row.get("ciudad") or ciudad_de(limpiar(html[:5000]))
            upd = {}
            if fecha:  upd["fecha_inicio"] = fecha
            if ciudad and not row.get("ciudad"): upd["ciudad"] = ciudad
            if upd:
                supabase.table("hackathons").update(upd).eq("id",row["id"]).execute()
                actualizados += 1
                print(f"   🔄 {row['nombre'][:60]} → {fecha}")
            time.sleep(0.35)
        print(f"   → {actualizados} actualizados")
    except Exception as e:
        print(f"   ⚠️ {e}")

# ────────────────────────────────────────────────────────────────
# DEDUPLICACIÓN por URL + similitud de título
# ────────────────────────────────────────────────────────────────
def normalizar_url(url):
    return re.sub(r'[/?#]$','', url.lower().split('?')[0])

def son_duplicados(a, b):
    if normalizar_url(a["url"]) == normalizar_url(b["url"]): return True
    ratio = difflib.SequenceMatcher(None, a["nombre"].lower(), b["nombre"].lower()).ratio()
    return ratio > 0.88

def deduplicar(eventos):
    unicos = []
    for ev in eventos:
        es_dup = False
        for ex in unicos:
            if son_duplicados(ev, ex):
                # Quedarse con el que tiene más datos
                if ev.get("fecha_inicio") and not ex.get("fecha_inicio"):
                    unicos[unicos.index(ex)] = ev
                es_dup = True
                break
        if not es_dup:
            unicos.append(ev)
    return unicos

# ────────────────────────────────────────────────────────────────
# GUARDAR EN SUPABASE
# ────────────────────────────────────────────────────────────────
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
                if ev.get("ciudad")       and not row.get("ciudad"):       upd["ciudad"]       = ev["ciudad"]
                if ev.get("fecha_inicio") and not row.get("fecha_inicio"): upd["fecha_inicio"] = ev["fecha_inicio"]
                if upd:
                    supabase.table("hackathons").update(upd).eq("id",row["id"]).execute()
                    actualizados += 1
                    print(f"   🔄 {ev['nombre'][:65]}")
        except Exception as e:
            print(f"   ❌ {ev['nombre'][:40]}: {e}")
    print(f"\n── ✅ Nuevos: {nuevos}  🔄 Actualizados: {actualizados}")

# ────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🚀 hackathons.es scraper v9 — definitivo")

    # 1. Limpiar BD de basura
    limpiar_bd()

    # 2. Actualizar sin fecha con JSON-LD
    actualizar_sin_fecha()

    # 3. Buscar nuevos eventos
    todos  = scrape_luma()
    todos += scrape_curated()

    # 4. Deduplicar
    todos = deduplicar(todos)
    print(f"\n📦 Total únicos tras dedup: {len(todos)}")

    # 5. Guardar
    print("\n💾 Guardando...")
    guardar(todos)
    print("\n✅ Listo")
