"""
arce_scraper.py v3.0
--------------------
Scraper G2B de ARCE con codigueras para nombres reales de organismos.
- Carga incisos, unidades ejecutoras y tipos de compra al inicio
- Cruza id_inciso + id_ue para mostrar nombre real del organismo
- URL y params segun Manual G2B v5.9
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

# ──────────────────────────────────────────────
# URLs segun Manual G2B v5.9
# ──────────────────────────────────────────────
BASE = "http://www.comprasestatales.gub.uy/comprasenlinea/jboss"
ARCE_REPORTE    = f"{BASE}/generarReporte"
ARCE_INCISOS    = f"{BASE}/reporteIncisos.do"
ARCE_UES        = f"{BASE}/reporteUnidadesEjecutoras.do"
ARCE_TIPOS      = f"{BASE}/reporteTiposCompra.do"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

MAX_ITEMS = 200

# ──────────────────────────────────────────────
# Rubros por palabras clave
# ──────────────────────────────────────────────
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

# ──────────────────────────────────────────────
# Carga de codigueras
# ──────────────────────────────────────────────
def cargar_xml_codiguera(url: str, client: httpx.Client) -> ET.Element | None:
    """Descarga y parsea una codiguera XML de ARCE."""
    try:
        resp = client.get(url, timeout=20)
        resp.raise_for_status()
        # Las codigueras usan ISO-8859-1 segun el manual
        content = resp.content
        for enc in [None, "iso-8859-1", "latin-1", "utf-8"]:
            try:
                root = ET.fromstring(content.decode(enc, errors="replace") if enc else content)
                return root
            except Exception:
                continue
        return None
    except Exception as e:
        log.warning(f"No se pudo cargar codiguera {url}: {e}")
        return None

def cargar_incisos(client: httpx.Client) -> dict:
    """
    Retorna {id_inciso: nom_inciso}
    Estructura: <incisos><inciso id-inciso="1" nom-inciso="Poder Legislativo"/></incisos>
    """
    log.info("Cargando codiguera: Incisos...")
    root = cargar_xml_codiguera(ARCE_INCISOS, client)
    if root is None:
        return {}
    result = {}
    for node in root.findall(".//inciso"):
        id_i = node.get("id-inciso") or node.get("id_inciso")
        nom  = node.get("nom-inciso") or node.get("nom_inciso")
        if id_i and nom:
            result[id_i.strip()] = nom.strip()
    log.info(f"  -> {len(result)} incisos cargados")
    return result

def cargar_unidades_ejecutoras(client: httpx.Client) -> dict:
    """
    Retorna {(id_inciso, id_ue): nom_ue}
    Estructura: <unidades-ejecutoras><unidad-ejecutora id-inciso="1" id-ue="1" nom-ue="Camara de Senadores"/></unidades-ejecutoras>
    """
    log.info("Cargando codiguera: Unidades Ejecutoras...")
    root = cargar_xml_codiguera(ARCE_UES, client)
    if root is None:
        return {}
    result = {}
    for node in root.findall(".//unidad-ejecutora"):
        id_i  = node.get("id-inciso") or node.get("id_inciso")
        id_ue = node.get("id-ue")     or node.get("id_ue")
        nom   = node.get("nom-ue")    or node.get("nom_ue")
        if id_i and id_ue and nom:
            result[(id_i.strip(), id_ue.strip())] = nom.strip()
    log.info(f"  -> {len(result)} unidades ejecutoras cargadas")
    return result

def cargar_tipos_compra(client: httpx.Client) -> dict:
    """
    Retorna {id_tipo: descripcion}
    Estructura: <tipos-compra><tipo-compra id="CD" descripcion="Compra Directa" .../></tipos-compra>
    """
    log.info("Cargando codiguera: Tipos de Compra...")
    root = cargar_xml_codiguera(ARCE_TIPOS, client)
    if root is None:
        return {}
    result = {}
    for node in root.findall(".//tipo-compra"):
        id_t = node.get("id")
        desc = node.get("descripcion")
        if id_t and desc:
            result[id_t.strip()] = desc.strip()
    log.info(f"  -> {len(result)} tipos de compra cargados")
    return result

def resolver_organismo(id_inciso, id_ue, incisos: dict, ues: dict) -> str:
    """
    Resuelve el nombre del organismo a partir de id_inciso e id_ue.
    Prioridad: nombre UE especifico > nombre inciso > fallback numerico
    """
    if not id_inciso:
        return ""
    
    id_i = str(id_inciso).strip()
    id_u = str(id_ue).strip() if id_ue else None

    # 1. Intentar nombre de la UE especifica
    if id_u:
        nom_ue = ues.get((id_i, id_u))
        if nom_ue:
            return nom_ue

    # 2. Fallback al nombre del inciso
    nom_inciso = incisos.get(id_i)
    if nom_inciso:
        return nom_inciso

    # 3. Fallback numerico
    return f"Organismo {id_i}"

# ──────────────────────────────────────────────
# Parseo de compras
# ──────────────────────────────────────────────
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
    """
    Dias completos que faltan desde HOY hasta la fecha de cierre.
    Usa ceil: si faltan 30 horas muestra 2 dias (no 1).
    Retorna 0 si ya cerro, None si no hay fecha.
    """
    if not fecha_iso:
        return None
    try:
        import math
        cierre = datetime.fromisoformat(fecha_iso)
        delta = (cierre - datetime.now()).total_seconds()
        if delta <= 0:
            return 0
        return math.ceil(delta / 86400)
    except (ValueError, TypeError):
        return None

def parse_xml_compras(xml_bytes, incisos: dict, ues: dict, tipos: dict) -> list[dict]:
    """
    Parsea el XML de compras enriqueciendo con nombres reales de codigueras.
    Estructura segun DTD del manual: reporte > reporte_dato > compra
    Atributos del nodo <compra>:
      id_compra, id_inciso, id_ue, id_tipocompra, num_compra, anio_compra,
      objeto, fecha_publicacion, fecha_hora_apertura, nombre_contacto, etc.
    """
    items = []
    root = None
    for enc in [None, "iso-8859-1", "latin-1", "utf-8"]:
        try:
            root = ET.fromstring(xml_bytes.decode(enc, errors="replace") if enc else xml_bytes)
            break
        except Exception:
            continue

    if root is None:
        log.warning("No se pudo parsear el XML de compras")
        return []

    compras = root.findall(".//compra") or root.findall(".//Compra")
    log.info(f"  -> {len(compras)} compras en XML")
    if compras:
        log.info(f"  Atributos muestra: {list(compras[0].keys())[:12]}")

    for c in compras:
        def a(*names):
            for n in names:
                v = c.get(n)
                if v and v.strip():
                    return v.strip()
                child = c.find(n)
                if child is not None and child.text and child.text.strip():
                    return child.text.strip()
            return None

        # Campos del manual G2B
        id_compra  = a("id_compra","id-compra")
        id_inciso  = a("id_inciso","id-inciso")
        id_ue      = a("id_ue","id-ue")
        id_tipo    = a("id_tipocompra","id-tipocompra","tipoCompra","tipo")
        num_compra = a("num_compra","num-compra","nroCompra","numero")
        anio       = a("anio_compra","anio-compra")
        objeto     = a("objeto","descripcion","objeto_compra")
        f_pub      = a("fecha_publicacion","fechaPublicacion","fecha-publicacion")
        f_apertura = a("fecha_hora_apertura","fechaApertura","fecha_hora_apertura","fechaCierre")
        f_tope     = a("fecha_hora_tope_entrega","fecha_hora_tope-entrega")
        nombre_pliego = a("nombre_pliego","nombre-pliego")
        contacto   = a("nombre_contacto","nombre-contacto")
        email      = a("email_contacto","email-contacto")

        # ID unico
        id_unico = id_compra or f"{id_tipo or 'X'}-{num_compra or ''}-{anio or ''}"

        # Nombre del organismo via codigueras
        nombre_org = resolver_organismo(id_inciso, id_ue, incisos, ues)

        # Nombre largo del tipo de compra
        tipo_desc = tipos.get(id_tipo, id_tipo) if id_tipo else "?"

        # Fechas
        fecha_pub_iso = parse_fecha(f_pub)
        # fecha_hora_apertura ES la fecha de cierre/apertura de ofertas
        # fecha_hora_tope_entrega es tope de entrega fisica (secundario)
        # Prioridad: apertura > tope_entrega
        fecha_cierre_iso = parse_fecha(f_apertura) or parse_fecha(f_tope)
        dias = dias_para_cierre(fecha_cierre_iso)

        # Monto (en compras vigentes suele no estar, viene en adjudicaciones)
        monto_raw = a("monto_adj","monto-adj","montoEstimado","monto")
        monto  = parse_monto(monto_raw)
        moneda_id = a("id_moneda_monto_adj","id-moneda-monto-adj","id_moneda","moneda") or "0"
        # 0 = pesos uruguayos segun codiguera monedas
        moneda = "UYU" if moneda_id in ("0", "UYU", "") else "USD" if moneda_id in ("1", "2") else moneda_id

        # URL al pliego
        if nombre_pliego:
            url_pliego = f"http://www.comprasestatales.gub.uy/Pliegos/{nombre_pliego}"
        elif id_compra:
            url_pliego = f"https://www.comprasestatales.gub.uy/comprasenlinea/compra/detalle?nroCompra={id_compra}"
        else:
            url_pliego = "https://www.comprasestatales.gub.uy"

        rubro = clasificar_rubro(objeto or "")

        items.append({
            "id":          id_unico,
            "idCompra":    id_compra,
            "nro":         num_compra,
            "anio":        anio,
            "tipo":        id_tipo or "?",
            "tipoNombre":  tipo_desc,
            "obj":         objeto or "",
            "org":         nombre_org,
            "orgId":       id_inciso or "",
            "ueId":        id_ue or "",
            "contacto":    contacto or "",
            "email":       email or "",
            "monto":       monto,
            "moneda":      moneda,
            "fechaPub":    fecha_pub_iso,
            "fechaCierre": fecha_cierre_iso,
            "dias":        dias,
            "rubro":       rubro,
            "url":         url_pliego,
            "nueva":       False,
        })

    return items

# ──────────────────────────────────────────────
# Fetch principal
# ──────────────────────────────────────────────
def fetch_todo(days_back=10) -> list[dict]:
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=min(days_back, 10))

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

    log.info(f"Rango: {start_date.strftime('%d/%m/%Y %H:%M')} -> {end_date.strftime('%d/%m/%Y %H:%M')}")

    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        # 1. Cargar codigueras primero
        incisos = cargar_incisos(client)
        ues     = cargar_unidades_ejecutoras(client)
        tipos   = cargar_tipos_compra(client)

        # 2. Fetch compras vigentes
        log.info(f"Consultando ARCE: {ARCE_REPORTE}")
        try:
            resp = client.get(ARCE_REPORTE, params=params)
            log.info(f"HTTP {resp.status_code} — {len(resp.content):,} bytes")
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            log.error(f"HTTP error: {e.response.status_code}")
            return []
        except httpx.RequestError as e:
            log.error(f"Error de conexion: {e}")
            return []

        return parse_xml_compras(resp.content, incisos, ues, tipos)

# ──────────────────────────────────────────────
# Post-procesamiento
# ──────────────────────────────────────────────
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
    monto_total = sum(l["monto"] for l in items if l.get("monto") and l.get("moneda") == "UYU")
    return {
        "total":    len(items),
        "nuevas24": len(nuevas),
        "urgentes": len(urgentes),
        "montoUYU": round(monto_total),
        "montoM":   round(monto_total / 1_000_000, 1),
    }

def filtrar_relevantes(items):
    validos = [l for l in items if l.get("obj") and l.get("org")]
    validos = [l for l in validos if not (isinstance(l.get("dias"), int) and l["dias"] < 0)]
    validos.sort(key=lambda l: (
        l.get("dias") if isinstance(l.get("dias"), int) else 999,
        -(l.get("monto") or 0)
    ))
    return validos[:MAX_ITEMS]

# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    repo_root   = Path(__file__).parent.parent
    output_path = repo_root / "data.json"

    log.info("=" * 50)
    log.info("LicitaBot UY — Scraper ARCE G2B v3.0")
    log.info(f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    log.info("=" * 50)

    data_anterior = {}
    if output_path.exists():
        try:
            with open(output_path, encoding="utf-8") as f:
                data_anterior = json.load(f)
            log.info(f"Data anterior: {len(data_anterior.get('licitaciones', []))} items")
        except Exception:
            log.warning("No se pudo leer data anterior")

    items_raw = fetch_todo(days_back=10)

    if not items_raw:
        log.error("ARCE no devolvio datos.")
        if data_anterior:
            log.info("Manteniendo data anterior.")
            sys.exit(0)
        items_raw = []

    items = marcar_nuevas(items_raw, data_anterior)
    items = filtrar_relevantes(items)
    stats = calcular_stats(items)

    log.info(f"Items finales: {len(items)}")
    log.info(f"Stats: {stats}")

    # Estadisticas por organismo (top 5)
    por_org = {}
    for l in items:
        org = l.get("org", "?")
        por_org[org] = por_org.get(org, 0) + 1
    top_orgs = sorted(por_org.items(), key=lambda x: -x[1])[:5]
    log.info(f"Top organismos: {top_orgs}")

    output = {
        "meta": {
            "actualizado": datetime.now().isoformat(),
            "fuente":      "comprasestatales.gub.uy",
            "licencia":    "Datos Abiertos — Licencia DAG Uruguay",
            "version":     "3.0",
            "total":       len(items),
        },
        "stats":        stats,
        "licitaciones": items,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f"data.json guardado — {output_path.stat().st_size / 1024:.1f} KB")
    log.info("OK.")

if __name__ == "__main__":
    main()
