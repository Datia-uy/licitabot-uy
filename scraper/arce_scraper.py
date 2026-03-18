"""
arce_scraper.py v3.1 - Edición Datia
--------------------
- Consulta segmentada día por día (evita el límite de 200/500 registros de ARCE)
- Cálculo preciso de días restantes hacia la Fecha de Apertura.
- Referencia temporal dinámica basada en 'hoy'.
"""

import xml.etree.ElementTree as ET
import json
import re
import sys
import logging
import math
from datetime import datetime, timedelta
from pathlib import Path

try:
    import httpx
except ImportError:
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "httpx"], check=True)
    import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("arce")

# Configuración
BASE = "http://www.comprasestatales.gub.uy/comprasenlinea/jboss"
ARCE_REPORTE    = f"{BASE}/generarReporte"
ARCE_INCISOS    = f"{BASE}/reporteIncisos.do"
ARCE_UES        = f"{BASE}/reporteUnidadesEjecutoras.do"
ARCE_TIPOS      = f"{BASE}/reporteTiposCompra.do"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Diccionario de Rubros (Simplificado para brevedad, mantener el tuyo original)
RUBROS = {
    "Tecnología e IT": ["software","hardware","informatica","sistema","red","datos","licencias"],
    "Construcción": ["construccion","obra","vial","infraestructura","remodelacion"],
    # ... (puedes mantener todos los que tenías)
}

def clasificar_rubro(texto):
    if not texto: return "Otros"
    t = texto.lower()
    for rubro, keywords in RUBROS.items():
        if any(kw in t for kw in keywords):
            return rubro
    return "Otros"

# ──────────────────────────────────────────────
# Carga de Codigueras
# ──────────────────────────────────────────────

def cargar_codigueras(client):
    log.info("Cargando maestros de ARCE...")
    
    # Auxiliar para descargar XML
    def get_xml(url):
        r = client.get(url, timeout=20)
        return ET.fromstring(r.content.decode('iso-8859-1', errors='replace'))

    # Incisos
    incisos = {n.get("id-inciso"): n.get("nom-inciso") for n in get_xml(ARCE_INCISOS).findall(".//inciso")}
    
    # UEs
    ues = {(n.get("id-inciso"), n.get("id-ue")): n.get("nom-ue") for n in get_xml(ARCE_UES).findall(".//unidad-ejecutora")}
    
    # Tipos
    tipos = {n.get("id"): n.get("descripcion") for n in get_xml(ARCE_TIPOS).findall(".//tipo-compra")}
    
    return incisos, ues, tipos

# ──────────────────────────────────────────────
# Lógica de Fechas y Parseo
# ──────────────────────────────────────────────

def dias_para_cierre(fecha_apertura_iso):
    """Calcula días restantes desde hoy hasta la apertura/cierre."""
    if not fecha_apertura_iso:
        return None
    try:
        apertura = datetime.fromisoformat(fecha_apertura_iso)
        hoy = datetime.now()
        delta = (apertura - hoy).total_seconds()
        # Si la fecha ya pasó, es 0 o negativo
        return math.ceil(delta / 86400) if delta > 0 else 0
    except:
        return None

def parse_fecha(val):
    if not val: return None
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).isoformat()
        except ValueError: continue
    return None

# ──────────────────────────────────────────────
# Core del Scraper
# ──────────────────────────────────────────────

def fetch_dia(client, fecha_target, incisos, ues, tipos):
    """Consulta un solo día específico para maximizar captura de datos."""
    params = {
        "tipo_publicacion": "lv",
        "anio_inicial": str(fecha_target.year),
        "mes_inicial":  f"{fecha_target.month:02d}",
        "dia_inicial":  f"{fecha_target.day:02d}",
        "hora_inicial": "00",
        "anio_final":   str(fecha_target.year),
        "mes_final":    f"{fecha_target.month:02d}",
        "dia_final":    f"{fecha_target.day:02d}",
        "hora_final":   "23",
    }
    
    try:
        resp = client.get(ARCE_REPORTE, params=params, timeout=40)
        resp.raise_for_status()
        root = ET.fromstring(resp.content.decode('iso-8859-1', errors='replace'))
        compras_nodos = root.findall(".//compra")
        
        items = []
        for c in compras_nodos:
            # Extracción de atributos con fallback
            id_i = c.get("id_inciso")
            id_u = c.get("id_ue")
            f_apertura = parse_fecha(c.get("fecha_hora_apertura"))
            
            # Resolver nombre organismo
            nombre_org = ues.get((id_i, id_u)) or incisos.get(id_i) or f"Organismo {id_i}"
            
            item = {
                "id": c.get("id_compra") or f"{c.get('id_tipocompra')}-{c.get('num_compra')}",
                "obj": c.get("objeto", ""),
                "org": nombre_org,
                "tipo": tipos.get(c.get("id_tipocompra"), c.get("id_tipocompra")),
                "fechaPub": parse_fecha(c.get("fecha_publicacion")),
                "fechaCierre": f_apertura,
                "dias": dias_para_cierre(f_apertura),
                "rubro": clasificar_rubro(c.get("objeto", "")),
                "url": f"https://www.comprasestatales.gub.uy/comprasenlinea/compra/detalle?nroCompra={c.get('id_compra')}"
            }
            items.append(item)
        return items
    except Exception as e:
        log.error(f"Error en día {fecha_target.strftime('%d/%m')}: {e}")
        return []

def main():
    repo_root = Path(__file__).parent.parent
    output_path = repo_root / "data.json"
    
    all_items = {}
    hoy = datetime.now()

    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        incisos, ues, tipos = cargar_codigueras(client)
        
        # Iteramos 10 días hacia atrás, uno por uno
        for i in range(11):
            target = hoy - timedelta(days=i)
            log.info(f"Scrapeando día: {target.strftime('%d/%m/%Y')}...")
            dia_items = fetch_dia(client, target, incisos, ues, tipos)
            
            # Usamos diccionario para evitar duplicados por ID
            for item in dia_items:
                all_items[item["id"]] = item
            
            log.info(f"  Acumulados: {len(all_items)} licitaciones")

    # Filtrado y Ordenado
    final_list = sorted(
        all_items.values(), 
        key=lambda x: (x['dias'] if x['dias'] is not None else 999)
    )

    # Guardar JSON
    output = {
        "meta": {
            "actualizado": hoy.isoformat(),
            "total": len(final_list),
            "version": "3.1"
        },
        "licitaciones": final_list
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    log.info(f"Proceso finalizado. Total guardado: {len(final_list)} licitaciones.")

if __name__ == "__main__":
    main()
