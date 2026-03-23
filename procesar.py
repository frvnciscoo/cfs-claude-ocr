"""
Procesador de contenedores -> Google Sheets
- Lee fotos desde la carpeta local 'Fotos'
- Filtra y extrae datos con Gemini 2.5 Flash
- Escribe directamente en Google Sheets
"""

import google.generativeai as genai
import gspread
from google.oauth2.service_account import Credentials
import io
import json
import os
from datetime import datetime
from PIL import Image

# -------------------------------------------------------
# CONFIGURACIÓN
# -------------------------------------------------------
GEMINI_KEY      = os.environ["GEMINI_KEY"]
GCP_CREDENTIALS = os.environ["GCP_CREDENTIALS"]

CARPETA_FOTOS   = "Fotos"
PROCESSED_FILE  = "processed.json"

# Datos de tu Google Sheet
SHEET_ID   = "1USS1t54xXnVnuJIUSmiU4U1TE55QcybyOedYyDvFfNA"
SHEET_NAME = "Sheet1"

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

# -------------------------------------------------------
# CONEXIÓN A GOOGLE SHEETS
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

# -------------------------------------------------------
# OPERACIONES LOCALES (Lectura de fotos)
# -------------------------------------------------------
def listar_fotos_locales() -> list[dict]:
    if not os.path.exists(CARPETA_FOTOS):
        os.makedirs(CARPETA_FOTOS)
        return []
    extensiones = {".jpg", ".jpeg", ".png", ".webp"}
    return [
        {"nombre": f, "ruta": os.path.join(CARPETA_FOTOS, f)}
        for f in os.listdir(CARPETA_FOTOS)
        if any(f.lower().endswith(ext) for ext in extensiones)
    ]

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
    todas_las_fotos = listar_fotos_locales()
    fotos_nuevas = [f for f in todas_las_fotos if f["nombre"] not in procesadas]

    if not fotos_nuevas:
        print("No hay fotos nuevas en la carpeta 'Fotos'. Fin.")
        return

    print(f"Fotos nuevas encontradas: {len(fotos_nuevas)}")
    
    # Conectar a Google Sheets
    hoja = conectar_sheets()
    
    # Si la hoja está vacía, poner encabezados
    if len(hoja.get_all_values()) == 0:
        hoja.append_row(["fecha", "archivo_origen", "status", "sigla", "numero", "dv", "max_gross_kg", "tara_kg"])

    filas_a_subir  = []
    nombres_ok     = set()
    procesadas_cnt = 0
    descartadas    = 0
    errores        = 0

    for foto in fotos_nuevas:
        nombre = foto["nombre"]
        print(f"  Procesando: {nombre}")

        try:
            img = preparar_imagen(foto["ruta"])

            if not es_puerta_contenedor(img):
                descartadas += 1
                filas_a_subir.append([datetime.now().strftime("%Y-%m-%d %H:%M"), nombre, "Descartada", "", "", "", "", ""])
                nombres_ok.add(nombre)
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

            nombres_ok.add(nombre)

        except Exception as e:
            errores += 1
            print(f"    → Error sistema: {e}")
            filas_a_subir.append([datetime.now().strftime("%Y-%m-%d %H:%M"), nombre, f"Error: {e}", "", "", "", "", ""])
            nombres_ok.add(nombre)

    # Enviar datos a Google Sheets de una sola vez
    if filas_a_subir:
        hoja.append_rows(filas_a_subir)
        print(f"Google Sheets actualizado con {len(filas_a_subir)} filas nuevas.")

    procesadas.update(nombres_ok)
    guardar_procesadas(procesadas)

    print(f"\n=== Resumen ===")
    print(f"  Extraídas:   {procesadas_cnt}")
    print(f"  Descartadas: {descartadas}")
    print(f"  Errores:     {errores}")

if __name__ == "__main__":
    main()
