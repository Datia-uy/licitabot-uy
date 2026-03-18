"""
email_sender.py
---------------
Lee suscriptores de una Google Sheet pública (CSV export),
los cruza con las licitaciones nuevas en data.json,
y envía alertas personalizadas por email via Gmail SMTP.

Costo: $0
Límite Gmail: 500 emails/día (suficiente para los primeros meses)
Cómo correr: python scraper/email_sender.py
Cómo automatizar: GitHub Actions lo corre después del scraper ARCE

Setup requerido (una sola vez):
1. Crear Google Form con los campos descritos abajo
2. La Sheet asociada se crea automáticamente
3. Publicar la Sheet como CSV (Archivo → Compartir → Publicar en web → CSV)
4. Pegar la URL en SHEET_CSV_URL
5. Generar contraseña de aplicación en Google:
   myaccount.google.com → Seguridad → Contraseñas de aplicación
6. Cargar como secrets en GitHub Actions:
   GMAIL_USER = tu@gmail.com
   GMAIL_APP_PASSWORD = xxxx xxxx xxxx xxxx (contraseña de app, no la normal)
"""

import json
import csv
import smtplib
import ssl
import os
import io
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

try:
    import httpx
except ImportError:
    import subprocess, sys
    subprocess.run([sys.executable, "-m", "pip", "install", "httpx"], check=True)
    import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("emailer")

# ──────────────────────────────────────────────────────
# CONFIGURACIÓN — editar estos valores
# ──────────────────────────────────────────────────────

# URL de exportación CSV de la Google Sheet.
# Cómo obtenerla:
# 1. Abrí la Sheet → Archivo → Compartir → Publicar en la Web
# 2. Seleccioná la hoja del Form → Formato: CSV → Publicar
# 3. Copiá la URL que aparece
SHEET_CSV_URL = os.getenv(
    "SHEET_CSV_URL",
    "https://docs.google.com/spreadsheets/d/TU_SHEET_ID/export?format=csv&gid=0"
)

# Credenciales Gmail (cargar como secrets en GitHub Actions)
GMAIL_USER     = os.getenv("GMAIL_USER",     "tu@gmail.com")
GMAIL_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")   # Contraseña de app, NO la normal

# Configuración del email
EMAIL_FROM_NAME   = "LicitaBot UY"
EMAIL_SUBJECT_TPL = "🏛️ {n} licitaciones nuevas para tu rubro — {fecha}"
DEMO_URL          = "https://TU_USUARIO.github.io/licitabot-uy"  # URL de tu GitHub Pages
WHATSAPP_CONTACT  = "https://wa.me/59899000000"  # Reemplazar con número real cuando lo tengas

# Máximo de licitaciones por email (para no abrumar)
MAX_LICITACIONES_POR_EMAIL = 5

# ──────────────────────────────────────────────────────
# Campos del Google Form
# El form debe tener exactamente estos campos (en cualquier orden):
#
# Pregunta 1: "Tu nombre"               → texto corto
# Pregunta 2: "Nombre de tu empresa"    → texto corto
# Pregunta 3: "Tu email"                → texto corto (con validación email)
# Pregunta 4: "Rubros de interés"       → casillas de verificación con opciones:
#               Tecnología e IT
#               Construcción e infraestructura
#               Salud e insumos médicos
#               Limpieza y mantenimiento
#               Seguridad y vigilancia
#               Logística y transporte
#               Consultoría y servicios
#               Alimentación y catering
#               Mobiliario y equipamiento
#               Todos los rubros
# Pregunta 5: "¿Tu empresa está inscripta en el RUPE?" → opción múltiple (Sí/No/En proceso)
# ──────────────────────────────────────────────────────

# Mapeo de columnas de la Sheet (índice base 0)
# La primera columna es siempre la fecha/hora del Form
COL_TIMESTAMP = 0
COL_NOMBRE    = 1
COL_EMPRESA   = 2
COL_EMAIL     = 3
COL_RUBROS    = 4   # Puede ser varias respuestas separadas por coma
COL_RUPE      = 5

TODOS_LOS_RUBROS = "Todos los rubros"


# ──────────────────────────────────────────────────────
# Carga de suscriptores desde Google Sheet
# ──────────────────────────────────────────────────────
def cargar_suscriptores() -> list[dict]:
    """
    Descarga el CSV de la Google Sheet y parsea los suscriptores.
    La Sheet se actualiza automáticamente cuando alguien llena el Form.
    """
    log.info(f"Descargando suscriptores desde Google Sheet...")
    try:
        resp = httpx.get(SHEET_CSV_URL, timeout=20, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        log.error(f"No se pudo descargar la Sheet: {e}")
        return []

    suscriptores = []
    reader = csv.reader(io.StringIO(resp.text))
    header = None

    for i, row in enumerate(reader):
        if i == 0:
            header = row
            log.info(f"Columnas del Form: {header}")
            continue

        if len(row) < COL_EMAIL + 1:
            continue

        email = row[COL_EMAIL].strip().lower()
        if not email or "@" not in email:
            continue

        nombre  = row[COL_NOMBRE].strip()  if len(row) > COL_NOMBRE  else ""
        empresa = row[COL_EMPRESA].strip() if len(row) > COL_EMPRESA else ""
        rubros_raw = row[COL_RUBROS].strip() if len(row) > COL_RUBROS else ""

        # Los rubros vienen como "Tecnología e IT, Construcción e infraestructura"
        rubros = [r.strip() for r in rubros_raw.split(",") if r.strip()]

        suscriptores.append({
            "nombre":  nombre  or "Estimado/a",
            "empresa": empresa or "",
            "email":   email,
            "rubros":  rubros,
            "todos":   TODOS_LOS_RUBROS in rubros or not rubros,
        })

    log.info(f"Suscriptores cargados: {len(suscriptores)}")
    # Deduplicar por email (mantener el más reciente = último en la Sheet)
    por_email = {}
    for s in suscriptores:
        por_email[s["email"]] = s
    return list(por_email.values())


# ──────────────────────────────────────────────────────
# Matching suscriptor ↔ licitaciones
# ──────────────────────────────────────────────────────
def licitaciones_para_suscriptor(suscriptor: dict, licitaciones: list[dict]) -> list[dict]:
    """
    Filtra las licitaciones relevantes para un suscriptor según sus rubros.
    Solo devuelve las marcadas como 'nueva' = True (publicadas en las últimas 24h).
    """
    nuevas = [l for l in licitaciones if l.get("nueva", False)]

    if suscriptor["todos"]:
        return nuevas[:MAX_LICITACIONES_POR_EMAIL]

    relevantes = [
        l for l in nuevas
        if l.get("rubro") in suscriptor["rubros"]
    ]
    return relevantes[:MAX_LICITACIONES_POR_EMAIL]


# ──────────────────────────────────────────────────────
# Templates de email
# ──────────────────────────────────────────────────────
def fmt_monto(n, moneda="UYU"):
    if not n:
        return "No informado"
    sym = "U$S" if moneda == "USD" else "$"
    if n >= 1_000_000:
        return f"{sym}{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{sym}{round(n/1_000)}K"
    return f"{sym}{round(n):,}"

def dias_label(d):
    if d is None:
        return "Fecha no disponible"
    if d == 0:
        return "⚠️ Cierra HOY"
    if d == 1:
        return "⚠️ Cierra MAÑANA"
    if d <= 7:
        return f"⚠️ Cierra en {d} días"
    return f"Cierra en {d} días"

def generar_html(suscriptor: dict, licitaciones: list[dict]) -> str:
    nombre  = suscriptor["nombre"].split()[0]  # Solo el primer nombre
    empresa = suscriptor["empresa"]
    fecha   = datetime.now().strftime("%d/%m/%Y")
    n       = len(licitaciones)

    filas_html = ""
    for l in licitaciones:
        urgencia_color = (
            "#dc2626" if isinstance(l.get("dias"), int) and l["dias"] <= 3 else
            "#b45309" if isinstance(l.get("dias"), int) and l["dias"] <= 7 else
            "#16a34a"
        )
        tipo_color = {
            "LP": "#1d4ed8", "LA": "#15803d",
            "CD": "#495057", "CP": "#92400e"
        }.get(l.get("tipo", ""), "#495057")

        filas_html += f"""
        <tr>
          <td style="padding:14px 0;border-bottom:1px solid #f1f3f5;vertical-align:top">
            <div style="margin-bottom:3px">
              <span style="background:#dbeafe;color:{tipo_color};font-family:monospace;font-size:11px;font-weight:600;padding:2px 7px;border-radius:4px">{l.get('tipo','')}</span>
              <span style="font-size:11px;color:#6c757d;margin-left:8px">{l.get('nro') or l.get('id','')}</span>
            </div>
            <div style="font-size:14px;color:#1a1a2e;font-weight:500;margin:4px 0 2px;line-height:1.4">{l.get('obj','')}</div>
            <div style="font-size:12px;color:#6c757d">{l.get('org','')}</div>
            <div style="margin-top:8px;display:flex;gap:16px;flex-wrap:wrap">
              <span style="font-size:12px;color:#1a1a2e"><strong>Monto:</strong> {fmt_monto(l.get('monto'), l.get('moneda'))}</span>
              <span style="font-size:12px;color:{urgencia_color}"><strong>{dias_label(l.get('dias'))}</strong></span>
              <span style="font-size:12px;color:#6c757d">{l.get('rubro','')}</span>
            </div>
            <div style="margin-top:8px">
              <a href="{l.get('url', 'https://www.comprasestatales.gub.uy')}" style="font-size:12px;color:#1d4ed8;text-decoration:none">Ver pliego completo →</a>
            </div>
          </td>
        </tr>
        """

    saludo_empresa = f" — <strong>{empresa}</strong>" if empresa else ""

    html = f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f8f9fa;font-family:system-ui,-apple-system,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f8f9fa;padding:32px 0">
<tr><td align="center">
<table width="580" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:10px;overflow:hidden;border:1px solid #e9ecef">

  <!-- HEADER -->
  <tr>
    <td style="background:#1a1a2e;padding:20px 28px;display:flex;align-items:center;justify-content:space-between">
      <span style="color:#fff;font-size:16px;font-weight:600;letter-spacing:-.3px">LicitaBot UY</span>
      <span style="color:#adb5bd;font-size:11px">{fecha}</span>
    </td>
  </tr>

  <!-- HERO -->
  <tr>
    <td style="padding:24px 28px 16px;border-bottom:1px solid #f1f3f5">
      <p style="font-size:15px;color:#1a1a2e;font-weight:500;margin:0 0 6px">
        Hola {nombre}{saludo_empresa} 👋
      </p>
      <p style="font-size:13px;color:#6c757d;margin:0;line-height:1.5">
        {'Encontramos <strong>' + str(n) + ' licitaciones nuevas</strong> del Estado uruguayo que coinciden con tus rubros.' if n > 1 else 'Encontramos <strong>1 licitación nueva</strong> del Estado uruguayo que coincide con tus rubros.'}
        Publicadas en las últimas 24 horas en <a href="https://www.comprasestatales.gub.uy" style="color:#1d4ed8">comprasestatales.gub.uy</a>.
      </p>
    </td>
  </tr>

  <!-- LICITACIONES -->
  <tr>
    <td style="padding:0 28px">
      <table width="100%" cellpadding="0" cellspacing="0">
        {filas_html}
      </table>
    </td>
  </tr>

  <!-- CTA -->
  <tr>
    <td style="padding:20px 28px;background:#f8f9fa;border-top:1px solid #e9ecef">
      <p style="font-size:13px;color:#1a1a2e;margin:0 0 12px;font-weight:500">
        ¿Querés ver todas las licitaciones vigentes y filtrar por organismo?
      </p>
      <a href="{DEMO_URL}" style="display:inline-block;background:#1a1a2e;color:#fff;font-size:13px;font-weight:500;padding:10px 20px;border-radius:7px;text-decoration:none;margin-right:8px">
        Ver monitor completo →
      </a>
    </td>
  </tr>

  <!-- WHATSAPP UPGRADE -->
  <tr>
    <td style="padding:16px 28px;background:#dcfce7;border-top:1px solid #bbf7d0">
      <p style="font-size:12px;color:#15803d;margin:0">
        💬 <strong>Próximamente:</strong> también podrás recibir estas alertas por WhatsApp al instante.
        Si querés ser de los primeros, <a href="{WHATSAPP_CONTACT}" style="color:#15803d;font-weight:500">escribinos acá</a>.
      </p>
    </td>
  </tr>

  <!-- FOOTER -->
  <tr>
    <td style="padding:16px 28px;border-top:1px solid #f1f3f5">
      <p style="font-size:11px;color:#adb5bd;margin:0;line-height:1.5">
        Recibís este email porque te suscribiste en <a href="{DEMO_URL}" style="color:#adb5bd">licitabot.uy</a>.
        Datos de <a href="https://www.comprasestatales.gub.uy" style="color:#adb5bd">comprasestatales.gub.uy</a> bajo Licencia de Datos Abiertos de Uruguay.
        <br><a href="{DEMO_URL}/unsub?email={suscriptor['email']}" style="color:#adb5bd">Desuscribirse</a>
      </p>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""
    return html

def generar_texto(suscriptor: dict, licitaciones: list[dict]) -> str:
    nombre = suscriptor["nombre"].split()[0]
    n      = len(licitaciones)
    fecha  = datetime.now().strftime("%d/%m/%Y")
    lines  = [
        f"LicitaBot UY — {fecha}",
        "=" * 40,
        f"Hola {nombre},",
        "",
        f"Encontramos {n} licitación{'es' if n > 1 else ''} nueva{'s' if n > 1 else ''} en tu rubro:",
        "",
    ]
    for l in licitaciones:
        lines += [
            f"[{l.get('tipo','')}] {l.get('nro') or l.get('id','')}",
            f"Organismo: {l.get('org','')}",
            f"Objeto: {l.get('obj','')}",
            f"Monto: {fmt_monto(l.get('monto'), l.get('moneda','UYU'))}",
            f"{dias_label(l.get('dias'))}",
            f"Ver: {l.get('url', 'https://www.comprasestatales.gub.uy')}",
            "",
        ]
    lines += [
        "─" * 40,
        f"Monitor completo: {DEMO_URL}",
        "",
        "Para desuscribirte respondé este email con 'BAJA'.",
    ]
    return "\n".join(lines)


# ──────────────────────────────────────────────────────
# Envío via Gmail SMTP
# ──────────────────────────────────────────────────────
def enviar_email(destinatario: dict, licitaciones: list[dict],
                 smtp: smtplib.SMTP_SSL) -> bool:
    """Envía el email de alerta a un suscriptor."""
    n     = len(licitaciones)
    fecha = datetime.now().strftime("%d/%m/%Y")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = EMAIL_SUBJECT_TPL.format(n=n, fecha=fecha)
    msg["From"]    = f"{EMAIL_FROM_NAME} <{GMAIL_USER}>"
    msg["To"]      = destinatario["email"]
    msg["Reply-To"] = GMAIL_USER

    # Texto plano como fallback
    part1 = MIMEText(generar_texto(destinatario, licitaciones), "plain", "utf-8")
    part2 = MIMEText(generar_html(destinatario, licitaciones),  "html",  "utf-8")
    msg.attach(part1)
    msg.attach(part2)

    try:
        smtp.sendmail(GMAIL_USER, destinatario["email"], msg.as_string())
        log.info(f"  ✓ Email enviado → {destinatario['email']} ({n} licitaciones)")
        return True
    except smtplib.SMTPException as e:
        log.error(f"  ✗ Error enviando a {destinatario['email']}: {e}")
        return False


# ──────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────
def main():
    repo_root = Path(__file__).parent.parent
    data_path = repo_root / "data.json"

    log.info("=" * 50)
    log.info("LicitaBot UY — Enviador de alertas por email")
    log.info(f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    log.info("=" * 50)

    # 1. Cargar licitaciones
    if not data_path.exists():
        log.error("data.json no encontrado. Correr arce_scraper.py primero.")
        return

    with open(data_path, encoding="utf-8") as f:
        data = json.load(f)

    licitaciones = data.get("licitaciones", [])
    nuevas = [l for l in licitaciones if l.get("nueva", False)]
    log.info(f"Licitaciones totales: {len(licitaciones)} | Nuevas: {len(nuevas)}")

    if not nuevas:
        log.info("Sin licitaciones nuevas hoy — no se envían emails.")
        return

    # 2. Cargar suscriptores
    if not SHEET_CSV_URL or "TU_SHEET_ID" in SHEET_CSV_URL:
        log.warning("SHEET_CSV_URL no configurado. Configura la URL de tu Google Sheet.")
        log.info("Simulando envío con datos de prueba...")
        suscriptores = [
            {"nombre": "Test", "empresa": "Mi Empresa", "email": GMAIL_USER,
             "rubros": ["Tecnología e IT"], "todos": False}
        ]
    else:
        suscriptores = cargar_suscriptores()

    if not suscriptores:
        log.info("Sin suscriptores en la Sheet.")
        return

    # 3. Verificar credenciales
    if not GMAIL_PASSWORD:
        log.error("GMAIL_APP_PASSWORD no configurado.")
        log.info("Setup: myaccount.google.com → Seguridad → Contraseñas de aplicación")
        return

    # 4. Conectar a Gmail SMTP y enviar
    log.info(f"Conectando a Gmail SMTP como {GMAIL_USER}...")
    context = ssl.create_default_context()
    enviados = 0
    sin_novedades = 0

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
            smtp.login(GMAIL_USER, GMAIL_PASSWORD)
            log.info(f"Conexión Gmail OK — procesando {len(suscriptores)} suscriptores")

            for sus in suscriptores:
                lics = licitaciones_para_suscriptor(sus, licitaciones)
                if not lics:
                    sin_novedades += 1
                    continue
                ok = enviar_email(sus, lics, smtp)
                if ok:
                    enviados += 1

    except smtplib.SMTPAuthenticationError:
        log.error("Error de autenticación Gmail.")
        log.error("Asegurate de usar una 'Contraseña de aplicación', no tu contraseña normal.")
        log.error("Generala en: myaccount.google.com → Seguridad → Contraseñas de aplicación")
        return
    except smtplib.SMTPException as e:
        log.error(f"Error SMTP: {e}")
        return

    log.info("-" * 50)
    log.info(f"Emails enviados:       {enviados}")
    log.info(f"Sin novedades (no enviado): {sin_novedades}")
    log.info(f"Total suscriptores:    {len(suscriptores)}")
    log.info("Proceso finalizado.")

if __name__ == "__main__":
    main()
