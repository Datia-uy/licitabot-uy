"""
arce_scraper.py
---------------
Scraper oficial de la Interfaz G2B de ARCE (comprasestatales.gub.uy).
Documentación: Manual_de_Interfaz_G2B_v5.9 — ARCE Uruguay.

Genera data.json con las licitaciones vigentes de los últimos 30 días.
Diseñado para correr como GitHub Actions cron job cada 24 horas.

Uso:
    python scraper/arce_scraper.py

Salida:
    data.json en la raíz del repositorio
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("arce")

# ──────────────────────────────────────────────
# Configuración del endpoint ARCE
# Documentación: sección 3.1.2 del Manual G2B
# ──────────────────────────────────────────────
ARCE_BASE = "https://www.comprasestatales.gub.uy/comprasenlinea/servlet"

# Parámetros documentados en el manual G2B
TIPO_PUB_VIGENTES = "VIG"   # Llamados vigentes
TIPO_PUB_TODOS    = "ALL"   # Todos los llamados
TIPO_PUB_ADJ      = "ADJ"   # Adjudicaciones

# Tipos de compra relevantes para el demo
TIPOS_OBJETIVO = [
    "LP",  # Licitación Pública
    "LA",  # Licitación Abreviada
    "CD",  # Compra Directa
    "CP",  # Concurso de Precios
]

# El API acepta máximo 10 días por request.
# Para cubrir 30 días hacemos 3 requests de 10 días.
DIAS_VENTANA = 10
VENTANAS     = 3       # 3 × 10 días = 30 días de cobertura
MAX_ITEMS    = 200     # Máximo de licitaciones en el JSON final

# ──────────────────────────────────────────────
# Clasificador de rubros por palabras clave
# ──────────────────────────────────────────────
RUBROS = {
    "Tecnología e IT": [
        "software","hardware","informática","tecnología","sistema","red","fibra",
        "telecomunicaciones","digital","servidor","nube","cloud","ciberseguridad",
        "firewall","desarrollo","datos","soporte técnico","mantenimiento informático",
        "licencias","equipamiento informático","computadora","laptop","pantalla",
    ],
    "Construcción e infraestructura": [
        "construcción","obra","vial","ruta","pavimento","puente","edificio",
        "infraestructura","refacción","ampliación","señalización","saneamiento",
        "agua potable","cloacas","distribución","red de distribución","ferroviaria",
        "portuaria","hormigón","cemento","materiales de construcción","redes",
    ],
    "Salud e insumos médicos": [
        "médico","salud","medicamento","insumo hospitalario","diagnóstico","implante",
        "quirúrgico","farmacéutico","vacuna","laboratorio","resonancia","tomografía",
        "equipo médico","dispositivo médico","ambulancia","reactivo","sangre",
    ],
    "Limpieza y mantenimiento": [
        "limpieza","mantenimiento","higiene","residuos","gestión de residuos",
        "baños","lavandería","desinfección","pintura","jardinería","espacios verdes",
        "parques","plazas","mantenimiento edilicio","conservación",
    ],
    "Seguridad y vigilancia": [
        "seguridad","vigilancia","guardia","custodia","monitoreo","cámara",
        "alarma","control de acceso","patrullaje","protección",
    ],
    "Logística y transporte": [
        "transporte","logística","vehículo","camión","camioneta","flota",
        "distribución","traslado","flete","correo","encomienda","combustible",
        "ómnibus","automóvil","ambulancia",
    ],
    "Consultoría y servicios": [
        "consultoría","asesoría","auditoría","capacitación","formación","estudio",
        "diseño","publicidad","comunicación","impresión","fotografía","traducción",
        "servicios profesionales","evaluación","diagnóstico organizacional",
    ],
    "Alimentación y catering": [
        "alimento","alimentación","catering","refrigerio","comida","provisión",
        "canasta","víveres","cocina","comedor","servicio de comidas",
    ],
    "Mobiliario y equipamiento": [
        "mobiliario","mueble","silla","escritorio","equipamiento","herramienta",
        "máquina","instrumento","electrodoméstico","climatización","aire acondicionado",
    ],
}

def clasificar_rubro(texto: str) -> str:
    """Asigna un rubro al objeto de la licitación por palabras clave."""
    t = texto.lower()
    for rubro, keywords in RUBROS.items():
        if any(kw in t for kw in keywords):
            return rubro
    return "Otros"

# ──────────────────────────────────────────────
# Parseo del XML de ARCE
# ──────────────────────────────────────────────
def parse_monto(val: str | None) -> float | None:
    """Limpia y convierte el monto a float."""
    if not val:
        return None
    # Eliminar puntos de miles, reemplazar coma decimal
    clean = re.sub(r"[^\d.,]", "", val).replace(".", "").replace(",", ".")
    try:
        return float(clean) if clean else None
    except ValueError:
        return None

def parse_fecha(val: str | None) -> str | None:
    """Normaliza fecha DD/MM/AAAA o DD/MM/AAAA HH:MM a ISO 8601."""
    if not val:
        return None
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).isoformat()
        except ValueError:
            continue
    return val

def dias_para_cierre(fecha_cierre_iso: str | None) -> int | None:
    """Calcula días restantes hasta el cierre."""
    if not fecha_cierre_iso:
        return None
    try:
        cierre = datetime.fromisoformat(fecha_cierre_iso)
        delta = (cierre - datetime.now()).days
        return max(delta, 0)
    except (ValueError, TypeError):
        return None

def parse_xml_arce(xml_text: str) -> list[dict]:
    """
    Parsea el XML de ARCE según la estructura documentada en Manual G2B v5.9.
    
    Estructura del XML (sección 3.1.4 del manual):
    <Compras>
      <compra nroCompra="..." tipoCompra="..." descripcion="..."
              organismo="..." unidadEjecutora="..." 
              montoEstimado="..." moneda="..."
              fechaPublicacion="..." fechaCierre="..."
              nroLicitacion="..." .../>
    </Compras>
    
    Nota: El manual documenta atributos en el nodo <compra>.
    Algunos pueden variar según la versión del portal.
    """
    items = []
    
    # El XML de ARCE a veces tiene declaración de encoding que httpx ya maneja
    # Intentar parseo directo, con fallback para XML malformado
    root = None
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        # Intentar limpiar caracteres problemáticos
        cleaned = xml_text.replace("&", "&amp;")
        try:
            root = ET.fromstring(cleaned)
        except ET.ParseError as e:
            log.warning(f"XML no parseable: {e}")
            return []

    # El nodo raíz puede ser <Compras> o variantes
    # Buscar todos los nodos <compra> independientemente del nivel
    compras = root.findall(".//compra")
    if not compras:
        # Intentar variantes del tag
        for tag in ["Compra", "COMPRA", "item", "licitacion"]:
            compras = root.findall(f".//{tag}")
            if compras:
                break

    log.info(f"  → {len(compras)} items en el XML")

    for c in compras:
        # Extraer atributos (según manual G2B los campos son atributos del nodo)
        # Usar .get() con fallback a texto del nodo hijo si existe
        def attr(name: str, *aliases) -> str | None:
            for n in [name] + list(aliases):
                v = c.get(n) or c.get(n.lower()) or c.get(n.upper())
                if v:
                    return v.strip()
                child = c.find(n) or c.find(n.lower()) or c.find(n.upper())
                if child is not None and child.text:
                    return child.text.strip()
            return None

        nro        = attr("nroCompra", "numero", "nro")
        tipo       = attr("tipoCompra", "tipo")
        desc       = attr("descripcion", "objeto", "descripcionCompra")
        organismo  = attr("organismo", "nombreOrganismo")
        ue         = attr("unidadEjecutora", "nombreUE")
        monto_raw  = attr("montoEstimado", "monto", "importeEstimado")
        moneda     = attr("moneda", "codigoMoneda") or "UYU"
        f_pub      = attr("fechaPublicacion", "fechaPub")
        f_cierre   = attr("fechaCierre", "fechaLimite", "fechaApertura")
        estado     = attr("estado", "estadoCompra") or "vigente"
        id_compra  = attr("id", "idCompra", "nroCompra")

        if not desc or not organismo:
            continue

        # Normalizar tipo a código corto
        tipo_norm = tipo or ""
        if "Pública" in tipo_norm or "Publica" in tipo_norm:
            tipo_cod = "LP"
        elif "Abreviada" in tipo_norm:
            tipo_cod = "LA"
        elif "Directa" in tipo_norm:
            tipo_cod = "CD"
        elif "Precios" in tipo_norm or "Concurso" in tipo_norm:
            tipo_cod = "CP"
        elif "Pregón" in tipo_norm or "Pregon" in tipo_norm:
            tipo_cod = "PG"
        else:
            tipo_cod = tipo_norm[:4] if tipo_norm else "OTR"

        monto_float = parse_monto(monto_raw)
        fecha_pub_iso = parse_fecha(f_pub)
        fecha_cierre_iso = parse_fecha(f_cierre)
        dias = dias_para_cierre(fecha_cierre_iso)

        # Construir ID único y URL de detalle
        id_unico = id_compra or nro or f"{tipo_cod}-{nro}"
        url_detalle = (
            f"https://www.comprasestatales.gub.uy/consultas/detalle/id/{id_unico}"
            if id_compra else
            f"https://www.comprasestatales.gub.uy/consultas/"
        )

        item = {
            "id":        id_unico,
            "tipo":      tipo_cod,
            "nro":       nro or "",
            "org":       organismo,
            "ue":        ue or "",
            "obj":       desc,
            "monto":     monto_float,
            "moneda":    moneda,
            "fechaPub":  fecha_pub_iso,
            "fechaCierre": fecha_cierre_iso,
            "dias":      dias,
            "estado":    estado.lower(),
            "rubro":     clasificar_rubro(desc),
            "url":       url_detalle,
            "nueva":     False,  # Se marca después comparando con sesión anterior
        }
        items.append(item)

    return items

# ──────────────────────────────────────────────
# Llamadas al API de ARCE
# ──────────────────────────────────────────────
def build_url(tipo_pub: str, tipo_compra: str,
              fecha_ini: datetime, fecha_fin: datetime) -> str:
    """
    Construye la URL del servlet según Manual G2B sección 3.1.2.
    
    URL base documentada:
    https://www.comprasestatales.gub.uy/comprasenlinea/servlet
    
    Parámetros documentados:
    - tipoPublicacion: VIG | ALL | ADJ
    - tipoCompra: LP | LA | CD | CP | "" (todos)
    - fechaInicio: DD/MM/AAAA
    - fechaFin:    DD/MM/AAAA
    """
    fmt = "%d/%m/%Y"
    params = {
        "tipoPublicacion": tipo_pub,
        "tipoCompra":      tipo_compra,
        "fechaInicio":     fecha_ini.strftime(fmt),
        "fechaFin":        fecha_fin.strftime(fmt),
    }
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{ARCE_BASE}?{qs}"

def fetch_ventana(tipo_compra: str, fecha_ini: datetime,
                  fecha_fin: datetime, client: httpx.Client) -> list[dict]:
    """Obtiene licitaciones de una ventana de 10 días."""
    url = build_url(TIPO_PUB_VIGENTES, tipo_compra, fecha_ini, fecha_fin)
    log.info(f"  GET {fecha_ini.strftime('%d/%m')} → {fecha_fin.strftime('%d/%m')} [{tipo_compra or 'ALL'}]")

    try:
        resp = client.get(url, timeout=30, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        log.warning(f"  HTTP {e.response.status_code} para {url}")
        return []
    except httpx.RequestError as e:
        log.warning(f"  Error de conexión: {e}")
        return []

    content_type = resp.headers.get("content-type", "")
    if "xml" not in content_type and "text" not in content_type:
        log.warning(f"  Respuesta no es XML: {content_type}")
        return []

    return parse_xml_arce(resp.text)

def fetch_licitaciones_vigentes() -> list[dict]:
    """
    Obtiene todas las licitaciones vigentes publicadas en los últimos 30 días.
    Hace múltiples requests de 10 días (máximo permitido por ARCE).
    """
    hoy = datetime.now()
    todos = []
    ids_vistos = set()

    with httpx.Client(
        headers={
            "User-Agent": "LicitaBotUY/1.0 (monitor de compras públicas; contacto: hola@agente.uy)",
            "Accept": "application/xml, text/xml, */*",
        },
        timeout=30,
    ) as client:
        for ventana_idx in range(VENTANAS):
            # Ventanas de 10 días hacia atrás desde hoy
            fin   = hoy - timedelta(days=ventana_idx * DIAS_VENTANA)
            ini   = fin  - timedelta(days=DIAS_VENTANA - 1)

            log.info(f"Ventana {ventana_idx + 1}/{VENTANAS}")
            items = fetch_ventana("", ini, fin, client)  # "" = todos los tipos

            for item in items:
                if item["id"] not in ids_vistos:
                    ids_vistos.add(item["id"])
                    todos.append(item)

    log.info(f"Total único antes de filtrar: {len(todos)}")
    return todos

# ──────────────────────────────────────────────
# Post-procesamiento
# ──────────────────────────────────────────────
def marcar_nuevas(items: list[dict], data_anterior: dict) -> list[dict]:
    """
    Marca como 'nueva' cualquier licitación que no estaba en la ejecución anterior.
    Compara por ID.
    """
    ids_anteriores = {l["id"] for l in data_anterior.get("licitaciones", [])}
    hoy = datetime.now()
    for item in items:
        # "Nueva" = apareció en las últimas 24h O no estaba en la ejecución anterior
        es_nueva_por_id = item["id"] not in ids_anteriores
        if item.get("fechaPub"):
            try:
                pub = datetime.fromisoformat(item["fechaPub"])
                es_nueva_por_fecha = (hoy - pub).total_seconds() < 86400
            except (ValueError, TypeError):
                es_nueva_por_fecha = False
        else:
            es_nueva_por_fecha = False
        item["nueva"] = es_nueva_por_id or es_nueva_por_fecha
    return items

def calcular_stats(items: list[dict]) -> dict:
    """Calcula estadísticas resumen."""
    vigentes = [l for l in items if l.get("estado") != "adjudicada"]
    nuevas   = [l for l in items if l.get("nueva")]
    urgentes = [l for l in items if isinstance(l.get("dias"), int) and l["dias"] <= 7]
    monto_total = sum(
        l["monto"] for l in items
        if l.get("monto") and l.get("moneda") in ("UYU", "")
    )

    return {
        "total":    len(vigentes),
        "nuevas24": len(nuevas),
        "urgentes": len(urgentes),
        "montoUYU": round(monto_total),
        "montoM":   round(monto_total / 1_000_000, 1),
    }

def filtrar_relevantes(items: list[dict]) -> list[dict]:
    """
    Filtra y ordena:
    1. Excluye items sin objeto o sin organismo
    2. Excluye los que ya pasaron su fecha de cierre
    3. Ordena por urgencia (dias asc), luego por monto desc
    4. Limita al máximo configurado
    """
    hoy = datetime.now()
    validos = []
    for l in items:
        if not l.get("obj") or not l.get("org"):
            continue
        # Excluir si ya cerró
        if isinstance(l.get("dias"), int) and l["dias"] < 0:
            continue
        validos.append(l)

    # Ordenar: primero urgentes, luego por monto
    def sort_key(l):
        dias = l.get("dias") if isinstance(l.get("dias"), int) else 999
        monto = l.get("monto") or 0
        return (dias, -monto)

    validos.sort(key=sort_key)
    return validos[:MAX_ITEMS]

# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    repo_root = Path(__file__).parent.parent
    output_path = repo_root / "data.json"

    log.info("=" * 50)
    log.info("LicitaBot UY — Scraper ARCE")
    log.info(f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    log.info("=" * 50)

    # Cargar data anterior para comparar novedades
    data_anterior = {}
    if output_path.exists():
        try:
            with open(output_path, encoding="utf-8") as f:
                data_anterior = json.load(f)
            log.info(f"Data anterior: {len(data_anterior.get('licitaciones', []))} items")
        except (json.JSONDecodeError, OSError):
            log.warning("No se pudo leer data anterior — tratando todo como nuevo")

    # Fetch desde ARCE
    log.info("Consultando ARCE...")
    items_raw = fetch_licitaciones_vigentes()

    if not items_raw:
        log.error("ARCE no devolvió datos. Abortando para no sobreescribir data válida.")
        # Si hay data anterior, no tocarla
        if data_anterior:
            log.info("Manteniendo data anterior intacta.")
            sys.exit(0)
        else:
            log.warning("Generando data.json vacío como fallback.")
            items_raw = []

    # Post-procesar
    items = marcar_nuevas(items_raw, data_anterior)
    items = filtrar_relevantes(items)
    stats = calcular_stats(items)

    log.info(f"Items finales: {len(items)}")
    log.info(f"Stats: {stats}")

    # Contar por tipo
    por_tipo = {}
    for l in items:
        t = l.get("tipo", "?")
        por_tipo[t] = por_tipo.get(t, 0) + 1
    log.info(f"Por tipo: {por_tipo}")

    # Construir JSON de salida
    output = {
        "meta": {
            "actualizado":  datetime.now().isoformat(),
            "fuente":       "comprasestatales.gub.uy",
            "licencia":     "Datos Abiertos — Licencia DAG Uruguay",
            "version":      "1.0",
            "total":        len(items),
        },
        "stats":       stats,
        "licitaciones": items,
    }

    # Guardar
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f"data.json guardado → {output_path} ({output_path.stat().st_size / 1024:.1f} KB)")
    log.info("Scraper finalizado exitosamente.")

if __name__ == "__main__":
    main()
