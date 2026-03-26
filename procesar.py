"""
Procesador de contenedores -> Google Sheets (Versión Red Local / Self-Hosted)
- Calcula automáticamente los días a buscar (Fines de semana o día anterior).
- Lee fotos directamente desde la ruta de red sin descargarlas.
- Filtra y extrae datos con Gemini 2.5 Flash.
- Escribe directamente en Google Sheets.
"""

import google.generativeai as genai
import gspread
from google.oauth2.service_account import Credentials
import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from PIL import Image

# -------------------------------------------------------
# CONFIGURACIÓN
# -------------------------------------------------------
GEMINI_KEY      = os.environ["GEMINI_KEY"]
GCP_CREDENTIALS = os.environ["GCP_CREDENTIALS"]

RUTA_SERVIDOR   = r"\\SvtiFileServer\Fotos Terminal\Consolidados"
PROCESSED_FILE  = "processed.json"

# Datos de tu Google Sheet
SHEET_ID   = "1USS1t54xXnVnuJIUSmiU4U1TE55QcybyOedYyDvFfNA"
SHEET_NAME = "Sheet1"

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

# -------------------------------------------------------
# LÓGICA DE FECHAS Y NAVEGACIÓN EN RED
# -------------------------------------------------------
def obtener_fechas_objetivo():
    """Retorna una lista de fechas (objetos date) según el día de la semana."""
    hoy = datetime.now().date()
    
    if hoy.weekday() == 0:  # 0 corresponde a Lunes
        print("Día detectado: Lunes. Buscando fotos de Viernes, Sábado y Domingo.")
        return [hoy - timedelta(days=3), hoy - timedelta(days=2), hoy - timedelta(days=1)]
    else:
        print("Día normal. Buscando fotos del día de ayer.")
        return [hoy - timedelta(days=1)]

def buscar_fotos_en_red() -> list[dict]:
    fechas_objetivo = obtener_fechas_objetivo()
    # Formateamos las fechas a DD-MM-YYYY para buscar las carpetas
    fechas_str = {f.strftime('%d-%m-%Y') for f in fechas_objetivo}
    anios_str = {str(f.year) for f in fechas_objetivo}

    base_dir = Path(RUTA_SERVIDOR)
    fotos_encontradas = []

    if not base_dir.exists():
        print(f"❌ Error crítico: No se puede acceder a la ruta de red: {base_dir}")
        print("Asegúrate de que el equipo (Self-Hosted Runner) tenga permisos y acceso a la red.")
        return fotos_encontradas

    extensiones = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}

    # Navegamos Año -> Mes -> Día
    for carpeta_anio in base_dir.iterdir():
        if not carpeta_anio.is_dir() or carpeta_anio.name not in anios_str: 
            continue

        for carpeta_mes in carpeta_anio.iterdir():
            if not carpeta_mes.is_dir(): 
                continue

            for carpeta_dia in carpeta_mes.iterdir():
                if not carpeta_dia.is_dir(): 
                    continue

                # Si el nombre de la carpeta coincide con alguna fecha que buscamos
                if carpeta_dia.name in fechas_str:
                    print(f"  📂 Escaneando directorio: {carpeta_dia.name}")
                    for archivo in carpeta_dia.rglob('*'):
                        if archivo.is_file() and archivo.suffix.lower() in extensiones:
                            fotos_encontradas.append({
                                "nombre": archivo.name,
                                "ruta": str(archivo),       # La ruta absoluta para que PIL la abra
                                "id_unico": str(archivo)    # Usaremos la ruta entera como ID en processed.json
                            })
                            
    return fotos_encontradas

# -------------------------------------------------------
# CONEXIÓN A GOOGLE SHEETS E HISTORIAL
# -------------------------------------------------------
def conectar_sheets():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds_dict = json.loads(GCP_CREDENTIALS)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    cliente = gspread.authorize(creds)
    return cliente.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

def cargar_procesadas() -> set:
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r") as f:
            return set(json.load(f))
    return set()

def guardar_procesadas(procesadas: set):
    with open(PROCESSED_FILE, "w") as f:
        json.dump(list(procesadas), f)

# -------------------------------------------------------
# UTILIDADES DE IMAGEN Y JSON
# -------------------------------------------------------
def preparar_imagen(ruta_imagen: str) -> Image.Image:
    with open(ruta_imagen, "rb") as f:
        img = Image.open(io.BytesIO(f.read()))
        if img.mode != "RGB":
            img = img.convert("RGB")
        if max(img.size) > 1024:
            img.thumbnail((1024, 1024), Image.LANCZOS)
        return img

def limpiar_json(texto: str):
    try:
        s = texto.strip()
        if "```json" in s:
            s = s.split("```json")[1].split("```")[0]
        elif "```" in s:
            s = s.split("```")[1].split("```")[0]
        return json.loads(s)
    except Exception:
        return None

# -------------------------------------------------------
# PIPELINE IA
# -------------------------------------------------------
def es_puerta_contenedor(img: Image.Image) -> bool:
    prompt = "Is this a photo of a shipping container door showing the container code/number? Answer only YES or NO."
    response = model.generate_content([prompt, img])
    return response.text.strip().upper().startswith("YES")

def extraer_datos_contenedor(img: Image.Image):
    prompt = """Actúa como OCR experto en logística. Extrae en JSON:
- sigla, numero, dv, max_gross_kg, tara_kg
Si no es legible pon null. Solo el JSON, sin explicaciones."""
    response = model.generate_content([prompt, img])
    return limpiar_json(response.text)

# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def main():
    print("=== Iniciando procesamiento a Google Sheets ===")

    procesadas = cargar_procesadas()
    todas_las_fotos = buscar_fotos_en_red()
    
    # Filtramos usando id_unico (la ruta completa) para no repetir
    fotos_nuevas = [f for f in todas_las_fotos if f["id_unico"] not in procesadas]

    if not fotos_nuevas:
        print("No hay fotos nuevas pendientes de procesar para las fechas objetivo. Fin.")
        return

    print(f"Fotos nuevas encontradas: {len(fotos_nuevas)}")
    
    hoja = conectar_sheets()
    
    if len(hoja.get_all_values()) == 0:
        hoja.append_row(["fecha", "archivo_origen", "status", "sigla", "numero", "dv", "max_gross_kg", "tara_kg"])

    filas_a_subir  = []
    ids_ok         = set()
    procesadas_cnt = 0
    descartadas    = 0
    errores        = 0

    for foto in fotos_nuevas:
        nombre = foto["nombre"]
        id_unico = foto["id_unico"]
        print(f"  Procesando: {nombre}")

        try:
            img = preparar_imagen(foto["ruta"])

            if not es_puerta_contenedor(img):
                descartadas += 1
                filas_a_subir.append([datetime.now().strftime("%Y-%m-%d %H:%M"), nombre, "Descartada", "", "", "", "", ""])
                ids_ok.add(id_unico)
                print(f"    → Descartada")
                continue

            datos = extraer_datos_contenedor(img)

            if datos:
                procesadas_cnt += 1
                filas_a_subir.append([
                    datetime.now().strftime("%Y-%m-%d %H:%M"), 
                    nombre, 
                    "OK", 
                    datos.get('sigla',''), 
                    datos.get('numero',''), 
                    datos.get('dv',''), 
                    datos.get('max_gross_kg',''), 
                    datos.get('tara_kg','')
                ])
                print(f"    → OK: {datos.get('sigla','?')}{datos.get('numero','?')}")
            else:
                errores += 1
                filas_a_subir.append([datetime.now().strftime("%Y-%m-%d %H:%M"), nombre, "Error OCR", "", "", "", "", ""])
                print(f"    → Error OCR")

            ids_ok.add(id_unico)

        except Exception as e:
            errores += 1
            print(f"    → Error sistema: {e}")
            filas_a_subir.append([datetime.now().strftime("%Y-%m-%d %H:%M"), nombre, f"Error: {e}", "", "", "", "", ""])
            ids_ok.add(id_unico)

    if filas_a_subir:
        hoja.append_rows(filas_a_subir)
        print(f"Google Sheets actualizado con {len(filas_a_subir)} filas nuevas.")

    procesadas.update(ids_ok)
    guardar_procesadas(procesadas)

    print(f"\n=== Resumen ===")
    print(f"  Extraídas:   {procesadas_cnt}")
    print(f"  Descartadas: {descartadas}")
    print(f"  Errores:     {errores}")

if __name__ == "__main__":
    main()
