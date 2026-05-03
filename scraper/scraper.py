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

# ── Queries ───────────────────────────────────────────────────
QUERIES = [
    "hackathon España 2026",
    "hackathon Madrid 2026",
    "hackathon Barcelona 2026",
    "hackathon Valencia 2026",
    "hackathon Sevilla 2026",
    "hackathon Bilbao 2026",
    "hackathon Málaga 2026",
    "hackathon universitario España 2026",
    "hackathon online España 2026",
    "hackathon inteligencia artificial España 2026",
]

# ── Ciudades ──────────────────────────────────────────────────
CIUDADES = [
    "Madrid","Barcelona","Valencia","Sevilla","Bilbao","Málaga","Zaragoza",
    "Murcia","Palma","Las Palmas","Alicante","Córdoba","Valladolid","Vigo",
    "Gijón","Granada","Pamplona","Salamanca","San Sebastián","Santander",
    "Toledo","Burgos","Oviedo","Albacete","Teruel","Cádiz","Huelva","Almería",
]

# ── Meses ─────────────────────────────────────────────────────
MESES = {
    "ene":1,"enero":1,"feb":2,"febrero":2,"mar":3,"marzo":3,
    "abr":4,"abril":4,"may":5,"mayo":5,"jun":6,"junio":6,
    "jul":7,"julio":7,"ago":8,"agosto":8,"sep":9,"septiembre":9,
    "oct":10,"octubre":10,"nov":11,"noviembre":11,"dic":12,"diciembre":12,
    "jan":1,"january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

# ── Extracción de fecha de un texto ──────────────────────────
def extraer_fecha(texto):
    t = texto.lower()

    # "25 de mayo de 2026" / "25 mayo 2026"
    for m in re.finditer(r'(\d{1,2})\s+(?:de\s+)?([a-záéíóú]+)\s+(?:de\s+)?(\d{4})', t):
        dia, mes_str, anio = int(m.group(1)), m.group(2)[:3], int(m.group(3))
        mes = MESES.get(mes_str)
        if mes and 2025 <= anio <= 2027 and 1 <= dia <= 31:
            try: return date(anio, mes, dia).isoformat()
            except ValueError: pass

    # "25/05/2026" o "25-05-2026"
    for m in re.finditer(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})', texto):
        dia, mes, anio = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2025 <= anio <= 2027 and 1 <= mes <= 12 and 1 <= dia <= 31:
            try: return date(anio, mes, dia).isoformat()
            except ValueError: pass

    # "2026-05-25" (ISO)
    for m in re.finditer(r'(\d{4})-(\d{2})-(\d{2})', texto):
        anio, mes, dia = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2025 <= anio <= 2027 and 1 <= mes <= 12 and 1 <= dia <= 31:
            try: return date(anio, mes, dia).isoformat()
            except ValueError: pass

    # "May 25, 2026"
    for m in re.finditer(r'([a-z]+)\s+(\d{1,2}),?\s+(\d{4})', t):
        mes_str, dia, anio = m.group(1)[:3], int(m.group(2)), int(m.group(3))
        mes = MESES.get(mes_str)
        if mes and 2025 <= anio <= 2027:
            try: return date(anio, mes, dia).isoformat()
            except ValueError: pass

    return None

# ── Extracción de ciudad ──────────────────────────────────────
def extraer_ciudad(texto):
    t = texto.lower()
    if any(w in t for w in ["online","virtual","remoto","digital","remote","worldwide"]):
        return "Online"
    for ciudad in CIUDADES:
        if ciudad.lower() in t:
            return ciudad
    return None

# ── Fetch de fecha desde la URL real ─────────────────────────
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; HackathonsESBot/1.0)"}

def fecha_desde_url(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=8, allow_redirects=True)
        if r.status_code != 200:
            return None
        # Buscar en el HTML completo
        html = r.text[:50000]  # primeros 50KB son suficientes
        fecha = extraer_fecha(html)
        return fecha
    except Exception:
        return None

# ── Búsqueda principal ────────────────────────────────────────
def buscar_hackathons():
    resultados = []
    urls_vistas = set()

    with DDGS() as ddgs:
        for query in QUERIES:
            try:
                hits = ddgs.text(query, max_results=10)
                for r in hits:
                    url = r.get("href","")
                    if not url or url in urls_vistas:
                        continue
                    urls_vistas.add(url)
                    texto = f"{r.get('title','')} {r.get('body','')}"
                    resultados.append({
                        "nombre":         r.get("title","Sin título")[:300],
                        "descripcion":    r.get("body","")[:1000],
                        "url":            url,
                        "ciudad":         extraer_ciudad(texto),
                        "online":         extraer_ciudad(texto) == "Online",
                        "fecha_inicio":   extraer_fecha(texto),
                        "fuente":         "DuckDuckGo",
                        "fecha_scraping": datetime.now().isoformat(),
                    })
            except Exception as e:
                print(f"⚠️  Error en '{query}': {e}")

    return resultados

# ── Guardar y enriquecer con fetch de páginas ─────────────────
def guardar_en_supabase(eventos):
    nuevos = actualizados = 0

    for ev in eventos:
        try:
            existing = supabase.table("hackathons")\
                .select("id,ciudad,fecha_inicio")\
                .eq("url", ev["url"])\
                .execute()

            # Calcular qué campos necesitamos actualizar/enriquecer
            falta_fecha  = not ev.get("fecha_inicio")
            falta_ciudad = not ev.get("ciudad")

            # Si falta la fecha, ir a buscarla a la página real
            if falta_fecha:
                print(f"   🌐 Buscando fecha en: {ev['url'][:60]}")
                fecha_web = fecha_desde_url(ev["url"])
                if fecha_web:
                    ev["fecha_inicio"] = fecha_web
                    falta_fecha = False
                    print(f"      ✓ Fecha encontrada: {fecha_web}")
                time.sleep(0.3)  # respetar rate limits

            if not existing.data:
                supabase.table("hackathons").insert(ev).execute()
                nuevos += 1
                print(f"✅ Nuevo:       {ev['nombre'][:65]}")
            else:
                row = existing.data[0]
                updates = {}
                if ev.get("ciudad")      and not row.get("ciudad"):
                    updates["ciudad"]      = ev["ciudad"]
                if ev.get("fecha_inicio") and not row.get("fecha_inicio"):
                    updates["fecha_inicio"] = ev["fecha_inicio"]

                if updates:
                    supabase.table("hackathons")\
                        .update(updates)\
                        .eq("id", row["id"])\
                        .execute()
                    actualizados += 1
                    print(f"🔄 Actualizado: {ev['nombre'][:65]}")
                else:
                    print(f"⏭️  Ya existe:  {ev['nombre'][:65]}")

        except Exception as e:
            print(f"❌ Error: {ev['nombre'][:40]} → {e}")

    print(f"\n── Resumen ──────────────────")
    print(f"   ✅ Nuevos:       {nuevos}")
    print(f"   🔄 Actualizados: {actualizados}")

# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🔍 Buscando hackathons...")
    eventos = buscar_hackathons()
    print(f"📦 Encontrados: {len(eventos)} únicos\n")
    guardar_en_supabase(eventos)
    print("\n✅ Scraper finalizado")
