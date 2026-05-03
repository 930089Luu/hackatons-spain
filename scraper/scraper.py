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

HEADERS = {"User-Agent": "Mozilla/5.0 (HackathonsESBot/2.0)"}

# ── Ciudades conocidas ────────────────────────────────────────
CIUDADES = [
    "Madrid","Barcelona","Valencia","Sevilla","Bilbao","Málaga","Zaragoza",
    "Murcia","Palma","Las Palmas","Alicante","Córdoba","Valladolid","Vigo",
    "Gijón","Granada","Pamplona","Salamanca","San Sebastián","Santander",
    "Toledo","Burgos","Oviedo","Albacete","Teruel","Cádiz","Huelva","Almería",
    "Donostia","Logroño","Castellón","Badajoz","Lleida","Tarragona","Girona",
]

MESES = {
    "ene":1,"enero":1,"feb":2,"febrero":2,"mar":3,"marzo":3,
    "abr":4,"abril":4,"may":5,"mayo":5,"jun":6,"junio":6,
    "jul":7,"julio":7,"ago":8,"agosto":8,"sep":9,"septiembre":9,
    "oct":10,"octubre":10,"nov":11,"noviembre":11,"dic":12,"diciembre":12,
    "jan":1,"january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

# ── Helpers ───────────────────────────────────────────────────
def extraer_ciudad(texto):
    t = texto.lower()
    if any(w in t for w in ["online","virtual","remoto","remote","worldwide","global"]):
        return "Online"
    for ciudad in CIUDADES:
        if ciudad.lower() in t:
            return ciudad
    return None

def extraer_fecha(texto):
    t = texto.lower()
    # "25 de mayo de 2026" / "25 mayo 2026"
    for m in re.finditer(r'(\d{1,2})\s+(?:de\s+)?([a-záéíóú]+)\s+(?:de\s+)?(\d{4})', t):
        dia, mes_str, anio = int(m.group(1)), m.group(2)[:3], int(m.group(3))
        mes = MESES.get(mes_str)
        if mes and 2025 <= anio <= 2027 and 1 <= dia <= 31:
            try: return date(anio, mes, dia).isoformat()
            except ValueError: pass
    # "2026-05-25" ISO
    for m in re.finditer(r'(202[5-7])-(\d{2})-(\d{2})', texto):
        try: return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
        except ValueError: pass
    # "25/05/2026"
    for m in re.finditer(r'(\d{1,2})[/\-](\d{1,2})[/\-](202[5-7])', texto):
        try: return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
        except ValueError: pass
    # "April 24, 2026" / "April 24-26, 2026"
    for m in re.finditer(r'([a-z]+)\s+(\d{1,2})(?:-\d+)?,?\s+(202[5-7])', t):
        mes = MESES.get(m.group(1)[:3])
        if mes:
            try: return date(int(m.group(3)), mes, int(m.group(2))).isoformat()
            except ValueError: pass
    return None

def es_hackathon(texto):
    """Filtra resultados que no son hackathons reales."""
    t = texto.lower()
    keywords = ["hackathon","hackaton","hackatón","hack ","hackfest","codeathon","datathon"]
    return any(k in t for k in keywords)


# ══════════════════════════════════════════════════════════════
# FUENTE 1: DEVPOST JSON API (sin API key, datos estructurados)
# ══════════════════════════════════════════════════════════════
def scrape_devpost():
    print("\n📡 Fuente 1: Devpost API...")
    eventos = []
    try:
        # Hackathons en España (upcoming)
        params = {
            "challenge_type": "all",
            "status[]": "upcoming",
            "order_by": "deadline",
            "per_page": 50,
            "location": "Spain",
        }
        r = requests.get("https://devpost.com/hackathons.json", params=params, headers=HEADERS, timeout=15)
        data = r.json()

        for h in data.get("hackathons", []):
            nombre   = h.get("title", "")
            url      = h.get("url", "")
            location = h.get("location", "") or ""
            dates_str = h.get("submission_period_dates", "") or ""
            ciudad   = extraer_ciudad(f"{nombre} {location}")
            fecha    = extraer_fecha(dates_str) or extraer_fecha(f"{nombre} {dates_str}")

            if not nombre or not url:
                continue

            eventos.append({
                "nombre":         nombre[:300],
                "descripcion":    h.get("tagline", "")[:1000],
                "url":            url,
                "ciudad":         ciudad,
                "online":         ciudad == "Online",
                "fecha_inicio":   fecha,
                "fuente":         "Devpost",
                "fecha_scraping": datetime.now().isoformat(),
            })
            print(f"   ✓ {nombre[:60]} | {fecha or 'sin fecha'} | {ciudad or '?'}")

        # También buscar hackathons online con temática España
        params2 = {
            "challenge_type": "all",
            "status[]": "upcoming",
            "order_by": "deadline",
            "per_page": 50,
            "themes[]": "spain",
        }
        r2 = requests.get("https://devpost.com/hackathons.json", params=params2, headers=HEADERS, timeout=15)
        data2 = r2.json()
        urls_ya = {e["url"] for e in eventos}
        for h in data2.get("hackathons", []):
            if h.get("url") not in urls_ya:
                nombre   = h.get("title", "")
                url      = h.get("url", "")
                dates_str = h.get("submission_period_dates", "") or ""
                ciudad   = extraer_ciudad(f"{nombre} {h.get('location','')}")
                fecha    = extraer_fecha(dates_str)
                if nombre and url:
                    eventos.append({
                        "nombre":         nombre[:300],
                        "descripcion":    h.get("tagline", "")[:1000],
                        "url":            url,
                        "ciudad":         ciudad,
                        "online":         ciudad == "Online",
                        "fecha_inicio":   fecha,
                        "fuente":         "Devpost",
                        "fecha_scraping": datetime.now().isoformat(),
                    })

    except Exception as e:
        print(f"   ⚠️ Error Devpost: {e}")

    print(f"   → {len(eventos)} eventos de Devpost")
    return eventos


# ══════════════════════════════════════════════════════════════
# FUENTE 2: HACKATHONSPAIN.COM (directorio específico España)
# ══════════════════════════════════════════════════════════════
def scrape_hackathonspain():
    print("\n📡 Fuente 2: HackathonSpain.com...")
    eventos = []
    try:
        r = requests.get("https://hackathonspain.com/calendario/", headers=HEADERS, timeout=15)
        html = r.text

        # Extraer bloques de eventos del HTML
        bloques = re.findall(
            r'<article[^>]*>(.*?)</article>',
            html, re.DOTALL | re.IGNORECASE
        )

        for bloque in bloques:
            # Nombre del evento
            nombre_m = re.search(r'<h\d[^>]*>(.*?)</h\d>', bloque, re.DOTALL)
            if not nombre_m:
                continue
            nombre = re.sub(r'<[^>]+>', '', nombre_m.group(1)).strip()
            if not nombre or not es_hackathon(nombre):
                continue

            # URL
            url_m = re.search(r'href=["\']([^"\']+)["\']', bloque)
            url = url_m.group(1) if url_m else ""
            if url and not url.startswith("http"):
                url = "https://hackathonspain.com" + url

            # Fecha y ciudad del texto del bloque
            texto_bloque = re.sub(r'<[^>]+>', ' ', bloque)
            fecha  = extraer_fecha(texto_bloque)
            ciudad = extraer_ciudad(f"{nombre} {texto_bloque}")

            if nombre:
                eventos.append({
                    "nombre":         nombre[:300],
                    "descripcion":    texto_bloque.strip()[:500],
                    "url":            url,
                    "ciudad":         ciudad,
                    "online":         ciudad == "Online",
                    "fecha_inicio":   fecha,
                    "fuente":         "HackathonSpain",
                    "fecha_scraping": datetime.now().isoformat(),
                })
                print(f"   ✓ {nombre[:60]} | {fecha or 'sin fecha'} | {ciudad or '?'}")

    except Exception as e:
        print(f"   ⚠️ Error HackathonSpain: {e}")

    print(f"   → {len(eventos)} eventos de HackathonSpain")
    return eventos


# ══════════════════════════════════════════════════════════════
# FUENTE 3: DUCKDUCKGO (con filtros estrictos)
# ══════════════════════════════════════════════════════════════

# Dominios de confianza para hackathons
DOMINIOS_OK = [
    "hackathon","hackaton","devpost","eventbrite","luma.events",
    "hackupc","hackspain","hackathonspain","junction","hfull",
    "dev.events","hackforgood","hackudc","malaga","ada.barcelona",
    "campus","universidad","unizar","upm","ugr","us.es","uam.es",
    "uca.es","ucm.es","uv.es","uab.es","upo.es","uma.es","ual.es",
]

QUERIES_DDG = [
    "hackathon España 2026 fecha",
    "hackathon Madrid Barcelona Valencia 2026",
    "hackathon universitario España 2026",
    "hackathon IA inteligencia artificial España 2026",
    "junction hackathon Spain 2026",
    "hackathon Sevilla Bilbao Málaga 2026",
]

def scrape_duckduckgo():
    print("\n📡 Fuente 3: DuckDuckGo (filtrado)...")
    eventos = []
    urls_vistas = set()

    with DDGS() as ddgs:
        for query in QUERIES_DDG:
            try:
                hits = ddgs.text(query, max_results=10)
                for r in hits:
                    url   = r.get("href", "")
                    titulo = r.get("title", "")
                    cuerpo = r.get("body", "")
                    texto  = f"{titulo} {cuerpo}"

                    if not url or url in urls_vistas:
                        continue

                    # Filtro 1: debe ser un hackathon real
                    if not es_hackathon(titulo):
                        continue

                    # Filtro 2: preferir dominios conocidos o exigir hackathon en URL
                    dominio = url.lower()
                    es_dominio_ok = any(d in dominio for d in DOMINIOS_OK)
                    if not es_dominio_ok and "hackath" not in dominio:
                        # Tolerar si el título es muy claro
                        if titulo.lower().count("hackathon") == 0:
                            continue

                    urls_vistas.add(url)
                    ciudad = extraer_ciudad(texto)
                    fecha  = extraer_fecha(texto)

                    # Si no tenemos fecha, intentar obtenerla de la página
                    if not fecha:
                        try:
                            rp = requests.get(url, headers=HEADERS, timeout=7)
                            if rp.status_code == 200:
                                fecha = extraer_fecha(rp.text[:40000])
                            time.sleep(0.3)
                        except Exception:
                            pass

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
                    print(f"   ✓ {titulo[:60]} | {fecha or 'sin fecha'} | {ciudad or '?'}")

            except Exception as e:
                print(f"   ⚠️ Error en '{query}': {e}")

    print(f"   → {len(eventos)} eventos de DuckDuckGo")
    return eventos


# ══════════════════════════════════════════════════════════════
# GUARDAR EN SUPABASE
# ══════════════════════════════════════════════════════════════
def guardar(eventos):
    nuevos = actualizados = 0

    for ev in eventos:
        if not ev.get("nombre") or not ev.get("url"):
            continue
        try:
            existing = supabase.table("hackathons")\
                .select("id,ciudad,fecha_inicio")\
                .eq("url", ev["url"])\
                .execute()

            if not existing.data:
                supabase.table("hackathons").insert(ev).execute()
                nuevos += 1
                print(f"   ✅ Nuevo:       {ev['nombre'][:65]}")
            else:
                row = existing.data[0]
                updates = {}
                if ev.get("ciudad")       and not row.get("ciudad"):
                    updates["ciudad"]       = ev["ciudad"]
                if ev.get("fecha_inicio")  and not row.get("fecha_inicio"):
                    updates["fecha_inicio"] = ev["fecha_inicio"]
                if updates:
                    supabase.table("hackathons")\
                        .update(updates).eq("id", row["id"]).execute()
                    actualizados += 1
                    print(f"   🔄 Actualizado: {ev['nombre'][:65]}")
        except Exception as e:
            print(f"   ❌ Error: {ev['nombre'][:40]} → {e}")

    print(f"\n── Resumen ──────────────────────────")
    print(f"   ✅ Nuevos:       {nuevos}")
    print(f"   🔄 Actualizados: {actualizados}")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 Iniciando scraper hackathons.es v3")

    todos = []
    todos += scrape_devpost()
    todos += scrape_hackathonspain()
    todos += scrape_duckduckgo()

    # Deduplicar por URL antes de guardar
    vistos = set()
    unicos = []
    for ev in todos:
        if ev["url"] not in vistos:
            vistos.add(ev["url"])
            unicos.append(ev)

    print(f"\n📦 Total únicos: {len(unicos)}")
    print("\n💾 Guardando en Supabase...")
    guardar(unicos)
    print("\n✅ Scraper finalizado")
