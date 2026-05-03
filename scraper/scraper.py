import os
import json
from datetime import datetime
from ddgs import DDGS
from supabase import create_client

# Credenciales desde variables de entorno
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Búsquedas a realizar cada día
QUERIES = [
    "hackathon España 2026",
    "hackathon Madrid 2026",
    "hackathon Barcelona 2026",
    "hackathon Valencia 2026",
    "hackathon universitario España 2026",
    "hackathon online España 2026",
]

def buscar_hackathons():
    resultados = []
    with DDGS() as ddgs:
        for query in QUERIES:
            try:
                hits = ddgs.text(query, max_results=10)
                for r in hits:
                    resultados.append({
                        "nombre": r.get("title", "Sin título"),
                        "descripcion": r.get("body", ""),
                        "url": r.get("href", ""),
                        "fuente": "DuckDuckGo",
                        "fecha_scraping": datetime.now().isoformat(),
                    })
            except Exception as e:
                print(f"Error en query '{query}': {e}")
    return resultados

def guardar_en_supabase(eventos):
    nuevos = 0
    for evento in eventos:
        # Comprobar si la URL ya existe (evitar duplicados)
        existing = supabase.table("hackathons")\
            .select("id")\
            .eq("url", evento["url"])\
            .execute()
        
        if not existing.data:
            supabase.table("hackathons").insert(evento).execute()
            nuevos += 1
            print(f"✅ Nuevo: {evento['nombre']}")
        else:
            print(f"⏭️ Ya existe: {evento['nombre']}")
    
    print(f"\nTotal nuevos eventos añadidos: {nuevos}")

if __name__ == "__main__":
    print("🔍 Buscando hackathons...")
    eventos = buscar_hackathons()
    print(f"📦 Encontrados: {len(eventos)} resultados")
    guardar_en_supabase(eventos)
    print("✅ Scraper finalizado")
