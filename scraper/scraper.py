import os
import re
from datetime import datetime, date
from ddgs import DDGS
from supabase import create_client

# ── Credenciales ──────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Queries de búsqueda ───────────────────────────────────────
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

# ── Ciudades conocidas ────────────────────────────────────────
CIUDADES = [
    "Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao",
    "Málaga", "Zaragoza", "Murcia", "Palma", "Las Palmas",
    "Alicante", "Córdoba", "Valladolid", "Vigo", "Gijón",
    "Granada", "Pamplona", "Salamanca", "San Sebastián",
    "Santander", "Toledo", "Burgos", "Oviedo", "Albacete",
    "Teruel", "Cádiz", "Huelva", "Jaén", "Almería",
]

# ── Meses en español e inglés ─────────────────────────────────
MESES = {
    "ene":1,"enero":1,"feb":2,"febrero":2,"mar":3,"marzo":3,
    "abr":4,"abril":4,"may":5,"mayo":5,"jun":6,"junio":6,
    "jul":7,"julio":7,"ago":8,"agosto":8,"sep":9,"septiembre":9,
    "oct":10,"octubre":10,"nov":11,"noviembre":11,"dic":12,"diciembre":12,
    "jan":1,"january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

# ── Extraer ciudad del texto ──────────────────────────────────
def extraer_ciudad(texto):
    t = texto.lower()
    if any(w in t for w in ["online", "virtual", "remoto", "digital", "remote", "web"]):
        return "Online"
    for ciudad in CIUDADES:
        if ciudad.lower() in t:
            return ciudad
    return None

# ── Extraer fecha del texto ───────────────────────────────────
def extraer_fecha(texto):
    t = texto.lower()

    # "25 de mayo de 2026" / "25 mayo 2026"
    p1 = r'(\d{1,2})\s+(?:de\s+)?([a-záéíóú]+)\s+(?:de\s+)?(\d{4})'
    for m in re.finditer(p1, t):
        dia, mes_str, anio = int(m.group(1)), m.group(2)[:3], int(m.group(3))
        mes = MESES.get(mes_str)
        if mes and 2025 <= anio <= 2027 and 1 <= dia <= 31:
            try:
                return date(anio, mes, dia).isoformat()
            except ValueError:
                pass

    # "25/05/2026" o "25-05-2026"
    p2 = r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})'
    for m in re.finditer(p2, texto):
        dia, mes, anio = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2025 <= anio <= 2027 and 1 <= mes <= 12 and 1 <= dia <= 31:
            try:
                return date(anio, mes, dia).isoformat()
            except ValueError:
                pass

    # "May 25, 2026" / "April 24"
    p3 = r'([a-z]+)\s+(\d{1,2}),?\s+(\d{4})'
    for m in re.finditer(p3, t):
        mes_str, dia, anio = m.group(1)[:3], int(m.group(2)), int(m.group(3))
        mes = MESES.get(mes_str)
        if mes and 2025 <= anio <= 2027:
            try:
                return date(anio, mes, dia).isoformat()
            except ValueError:
                pass

    return None

# ── Búsqueda con DuckDuckGo ───────────────────────────────────
def buscar_hackathons():
    resultados = []
    urls_vistas = set()

    with DDGS() as ddgs:
        for query in QUERIES:
            try:
                hits = ddgs.text(query, max_results=10)
                for r in hits:
                    url = r.get("href", "")
                    if not url or url in urls_vistas:
                        continue
                    urls_vistas.add(url)

                    texto = f"{r.get('title', '')} {r.get('body', '')}"
                    ciudad     = extraer_ciudad(texto)
                    fecha      = extraer_fecha(texto)
                    es_online  = (ciudad == "Online")

                    resultados.append({
                        "nombre":         r.get("title", "Sin título")[:300],
                        "descripcion":    r.get("body",  "")[:1000],
                        "url":            url,
                        "ciudad":         ciudad,
                        "online":         es_online,
                        "fecha_inicio":   fecha,
                        "fuente":         "DuckDuckGo",
                        "fecha_scraping": datetime.now().isoformat(),
                    })
            except Exception as e:
                print(f"⚠️  Error en '{query}': {e}")

    return resultados

# ── Guardar / actualizar en Supabase ─────────────────────────
def guardar_en_supabase(eventos):
    nuevos = actualizados = 0

    for ev in eventos:
        try:
            existing = supabase.table("hackathons") \
                .select("id, ciudad, fecha_inicio") \
                .eq("url", ev["url"]) \
                .execute()

            if not existing.data:
                supabase.table("hackathons").insert(ev).execute()
                nuevos += 1
                print(f"✅ Nuevo:      {ev['nombre'][:70]}")
            else:
                row     = existing.data[0]
                updates = {}
                if ev.get("ciudad")      and not row.get("ciudad"):
                    updates["ciudad"] = ev["ciudad"]
                if ev.get("fecha_inicio") and not row.get("fecha_inicio"):
                    updates["fecha_inicio"] = ev["fecha_inicio"]

                if updates:
                    supabase.table("hackathons") \
                        .update(updates) \
                        .eq("id", row["id"]) \
                        .execute()
                    actualizados += 1
                    print(f"🔄 Actualizado: {ev['nombre'][:70]}")
                else:
                    print(f"⏭️  Ya existe:  {ev['nombre'][:70]}")

        except Exception as e:
            print(f"❌ Error: {ev['nombre'][:40]} → {e}")

    print(f"\n── Resumen ──────────────────────")
    print(f"   ✅ Nuevos:      {nuevos}")
    print(f"   🔄 Actualizados: {actualizados}")

# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🔍 Buscando hackathons...")
    eventos = buscar_hackathons()
    print(f"📦 Encontrados: {len(eventos)} resultados únicos\n")
    guardar_en_supabase(eventos)
    print("\n✅ Scraper finalizado")
