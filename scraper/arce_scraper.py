"""
arce_scraper.py
---------------
Scraper oficial de la Interfaz G2B de ARCE (comprasestatales.gub.uy).
URL y parametros segun Manual G2B v5.9 y g2b_client.py de referencia.
"""

import xml.etree.ElementTree as ET
import json
import re
import sys
import logging
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

# URL CORRECTA segun Manual G2B y g2b_client.py de referencia
ARCE_BASE = "http://www.comprasestatales.gub.uy/comprasenlinea/jboss/generarReporte"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

MAX_ITEMS = 200

RUBROS = {
    "Tecnologia e IT": ["software","hardware","informatica","tecnologia","sistema","red","telecomunicaciones","digital","servidor","nube","cloud","ciberseguridad","firewall","desarrollo","datos","soporte tecnico","licencias","equipamiento informatico","computadora","laptop"],
    "Construccion e infraestructura": ["construccion","obra","vial","ruta","pavimento","puente","edificio","infraestructura","refaccion","ampliacion","senalizacion","saneamiento","agua potable","hormigon","cemento","materiales de construccion"],
    "Salud e insumos medicos": ["medico","salud","medicamento","insumo hospitalario","diagnostico","implante","quirurgico","farmaceutico","vacuna","laboratorio","resonancia","tomografia","equipo medico","dispositivo medico"],
    "Limpieza y mantenimiento": ["limpieza","mantenimiento","higiene","residuos","banos","lavanderia","desinfeccion","pintura","jardineria","espacios verdes","mantenimiento edilicio","conservacion"],
    "Seguridad y vigilancia": ["seguridad","vigilancia","guardia","custodia","monitoreo","camara","alarma","control de acceso","patrullaje","proteccion"],
    "Logistica y transporte": ["transporte","logistica","vehiculo","camion","camioneta","flota","distribucion","traslado","flete","combustible","omnibus","automovil"],
    "Consultoria y servicios": ["consultoria","asesoria","auditoria","capacitacion","formacion","estudio","diseno","publicidad","comunicacion","impresion","servicios profesionales","evaluacion"],
    "Alimentacion y catering": ["alimento","alimentacion","catering","refrigerio","comida","provision","canasta","viveres","cocina","comedor"],
    "Mobiliario y equipamiento": ["mobiliario","mueble","silla","escritorio","equipamiento","herramienta","maquina","instrumento","climatizacion","aire acondicionado"],
}

def clasificar_rubro(texto):
    if not texto:
        return "Otros"
    t = texto.lower()
    for a, b in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ñ","n")]:
        t = t.replace(a, b)
    for rubro, keywords in RUBROS.items():
        if any(kw in t for kw in keywords):
            return rubro
    return "Otros"

def parse_monto(val):
    if not val:
        return None
    clean = re.sub(r"[^\d.,]", "", str(val)).replace(".", "").replace(",", ".")
    try:
        return float(clean) if clean else None
    except ValueError:
        return None

def parse_fecha(val):
    if not val:
        return None
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).isoformat()
        except ValueError:
            continue
    return val

def dias_para_cierre(fecha_iso):
    if not fecha_iso:
        return None
    try:
        return max((datetime.fromisoformat(fecha_iso) - datetime.now()).days, 0)
    except (ValueError, TypeError):
        return None

def parse_xml_arce(xml_bytes):
    items = []
    root = None
    for encoding in [None, "latin-1", "utf-8"]:
        try:
            if encoding:
                root = ET.fromstring(xml_bytes.decode(encoding, errors="replace"))
            else:
                root = ET.fromstring(xml_bytes)
            break
        except Exception:
            continue

    if root is None:
        log.warning("No se pudo parsear el XML")
        return []

    compras = root.findall(".//compra") or root.findall(".//Compra") or root.findall(".//COMPRA")
    log.info(f"  -> {len(compras)} compras en XML")

    if compras:
        log.info(f"  Atributos del primer nodo: {list(compras[0].keys())}")

    for c in compras:
        def attr(*names):
            for n in names:
                v = c.get(n)
                if v and v.strip():
                    return v.strip()
                child = c.find(n)
                if child is not None and child.text and child.text.strip():
                    return child.text.strip()
            return None

        nro       = attr("nroCompra","numero","nro","id")
        tipo      = attr("tipoCompra","tipo","tipo-compra")
        desc      = attr("descripcion","objeto","descripcionCompra","nombre")
        organismo = attr("organismo","nombreOrganismo","nom-organismo","inciso")
        ue        = attr("unidadEjecutora","nombreUE","nom-ue","ue")
        monto_raw = attr("montoEstimado","monto","importeEstimado","importe")
        moneda    = attr("moneda","tipoMoneda") or "UYU"
        f_pub     = attr("fechaPublicacion","fechaPub","fecha-publicacion")
        f_cierre  = attr("fechaCierre","fechaApertura","fecha-cierre","fecha-apertura")
        nro_lic   = attr("nroLicitacion","nroLic","numero-licitacion")

        id_unico = nro or f"{tipo or 'X'}-{nro_lic or (desc[:20] if desc else 'sin-id')}"
        fecha_pub_iso    = parse_fecha(f_pub)
        fecha_cierre_iso = parse_fecha(f_cierre)
        monto  = parse_monto(monto_raw)
        dias   = dias_para_cierre(fecha_cierre_iso)
        rubro  = clasificar_rubro(desc or "")
        url    = f"https://www.comprasestatales.gub.uy/comprasenlinea/compra/detalle?nroCompra={nro}" if nro else "https://www.comprasestatales.gub.uy"

        items.append({
            "id": id_unico, "nro": nro, "nroLic": nro_lic,
            "tipo": tipo or "?", "obj": desc or "", "org": organismo or "",
            "ue": ue or "", "monto": monto, "moneda": moneda,
            "fechaPub": fecha_pub_iso, "fechaCierre": fecha_cierre_iso,
            "dias": dias, "rubro": rubro, "url": url, "nueva": False,
        })
    return items

def fetch_licitaciones(days_back=10):
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=min(days_back, 10))

    # Parametros CORRECTOS segun manual G2B
    params = {
        "tipo_publicacion": "lv",
        "anio_inicial": str(start_date.year),
        "mes_inicial":  f"{start_date.month:02d}",
        "dia_inicial":  f"{start_date.day:02d}",
        "hora_inicial": f"{start_date.hour:02d}",
        "anio_final":   str(end_date.year),
        "mes_final":    f"{end_date.month:02d}",
        "dia_final":    f"{end_date.day:02d}",
        "hora_final":   f"{end_date.hour:02d}",
    }

    log.info(f"URL: {ARCE_BASE}")
    log.info(f"Rango: {start_date.strftime('%d/%m/%Y')} -> {end_date.strftime('%d/%m/%Y')}")

    try:
        with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
            resp = client.get(ARCE_BASE, params=params)
            log.info(f"HTTP {resp.status_code} — {len(resp.content)} bytes")
            resp.raise_for_status()
            return parse_xml_arce(resp.content)
    except httpx.HTTPStatusError as e:
        log.error(f"HTTP error: {e.response.status_code}")
        return []
    except httpx.RequestError as e:
        log.error(f"Error de conexion: {e}")
        return []

def marcar_nuevas(items, data_anterior):
    ids_anteriores = {l["id"] for l in data_anterior.get("licitaciones", [])}
    hoy = datetime.now()
    for item in items:
        es_nueva_id = item["id"] not in ids_anteriores
        es_nueva_fecha = False
        if item.get("fechaPub"):
            try:
                pub = datetime.fromisoformat(item["fechaPub"])
                es_nueva_fecha = (hoy - pub).total_seconds() < 86400
            except Exception:
                pass
        item["nueva"] = es_nueva_id or es_nueva_fecha
    return items

def calcular_stats(items):
    nuevas   = [l for l in items if l.get("nueva")]
    urgentes = [l for l in items if isinstance(l.get("dias"), int) and l["dias"] <= 7]
    monto_total = sum(l["monto"] for l in items if l.get("monto") and l.get("moneda") in ("UYU",""))
    return {
        "total": len(items), "nuevas24": len(nuevas), "urgentes": len(urgentes),
        "montoUYU": round(monto_total), "montoM": round(monto_total / 1_000_000, 1),
    }

def filtrar_relevantes(items):
    validos = [l for l in items if l.get("obj") and l.get("org")]
    validos = [l for l in validos if not (isinstance(l.get("dias"), int) and l["dias"] < 0)]
    validos.sort(key=lambda l: (l.get("dias") if isinstance(l.get("dias"), int) else 999, -(l.get("monto") or 0)))
    return validos[:MAX_ITEMS]

def main():
    repo_root   = Path(__file__).parent.parent
    output_path = repo_root / "data.json"

    log.info("=" * 50)
    log.info("LicitaBot UY — Scraper ARCE G2B")
    log.info(f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    log.info("=" * 50)

    data_anterior = {}
    if output_path.exists():
        try:
            with open(output_path, encoding="utf-8") as f:
                data_anterior = json.load(f)
            log.info(f"Data anterior: {len(data_anterior.get('licitaciones',[]))} items")
        except Exception:
            log.warning("No se pudo leer data anterior")

    items_raw = fetch_licitaciones(days_back=10)

    if not items_raw:
        log.error("ARCE no devolvio datos.")
        if data_anterior:
            log.info("Manteniendo data anterior.")
            sys.exit(0)
        items_raw = []

    items = marcar_nuevas(items_raw, data_anterior)
    items = filtrar_relevantes(items)
    stats = calcular_stats(items)

    log.info(f"Items finales: {len(items)} | Stats: {stats}")

    output = {
        "meta": {"actualizado": datetime.now().isoformat(), "fuente": "comprasestatales.gub.uy", "version": "2.0", "total": len(items)},
        "stats": stats,
        "licitaciones": items,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f"data.json guardado — {output_path.stat().st_size / 1024:.1f} KB")
    log.info("OK.")

if __name__ == "__main__":
    main()
