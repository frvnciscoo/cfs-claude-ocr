"""
Procesador de contenedores (VERSIÓN PRUEBA GITHUB)
- Lee fotos desde la carpeta local 'Fotos'
- Filtra y extrae datos con Gemini 2.5 Flash
- Actualiza un Excel local 'reporte.xlsx'
"""

import google.generativeai as genai
import io
import json
import os
from datetime import datetime
from PIL import Image
import openpyxl

# -------------------------------------------------------
# CONFIGURACIÓN
# -------------------------------------------------------
GEMINI_KEY     = os.environ["GEMINI_KEY"]
CARPETA_FOTOS  = "Fotos"
EXCEL_PATH     = "reporte.xlsx"
PROCESSED_FILE = "processed.json"

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

# -------------------------------------------------------
# OPERACIONES LOCALES
# -------------------------------------------------------

def listar_fotos_locales() -> list[dict]:
    if not os.path.exists(CARPETA_FOTOS):
        os.makedirs(CARPETA_FOTOS) # Crea la carpeta si no existe
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
# ACTUALIZAR EXCEL LOCAL
# -------------------------------------------------------

def actualizar_excel_local(filas: list[dict]):
    if os.path.exists(EXCEL_PATH):
        wb = openpyxl.load_workbook(EXCEL_PATH)
        ws = wb.active
    else:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["fecha", "archivo_origen", "status", "sigla", "numero", "dv", "max_gross_kg", "tara_kg"])

    for fila in filas:
        ws.append([
            fila.get("fecha", ""),
            fila.get("archivo_origen", ""),
            fila.get("status", ""),
            fila.get("sigla", ""),
            fila.get("numero", ""),
            fila.get("dv", ""),
            fila.get("max_gross_kg", ""),
            fila.get("tara_kg", ""),
        ])
    
    wb.save(EXCEL_PATH)

# -------------------------------------------------------
# MAIN
# -------------------------------------------------------

def main():
    print("=== Iniciando procesamiento LOCAL ===")

    procesadas = cargar_procesadas()
    todas_las_fotos = listar_fotos_locales()
    fotos_nuevas = [f for f in todas_las_fotos if f["nombre"] not in procesadas]

    if not fotos_nuevas:
        print("No hay fotos nuevas en la carpeta 'Fotos'. Fin.")
        return

    print(f"Fotos nuevas encontradas: {len(fotos_nuevas)}")

    filas_nuevas   = []
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
                filas_nuevas.append({"fecha": datetime.now().strftime("%Y-%m-%d %H:%M"), "archivo_origen": nombre, "status": "Descartada"})
                nombres_ok.add(nombre)
                print(f"    → Descartada")
                continue

            datos = extraer_datos_contenedor(img)

            if datos:
                procesadas_cnt += 1
                filas_nuevas.append({"fecha": datetime.now().strftime("%Y-%m-%d %H:%M"), "archivo_origen": nombre, "status": "OK", **datos})
                print(f"    → OK: {datos.get('sigla','?')}{datos.get('numero','?')}")
            else:
                errores += 1
                filas_nuevas.append({"fecha": datetime.now().strftime("%Y-%m-%d %H:%M"), "archivo_origen": nombre, "status": "Error OCR"})
                print(f"    → Error OCR")

            nombres_ok.add(nombre)

        except Exception as e:
            errores += 1
            print(f"    → Error sistema: {e}")
            filas_nuevas.append({"fecha": datetime.now().strftime("%Y-%m-%d %H:%M"), "archivo_origen": nombre, "status": f"Error: {e}"})
            nombres_ok.add(nombre)

    if filas_nuevas:
        actualizar_excel_local(filas_nuevas)
        print(f"Excel actualizado con {len(filas_nuevas)} filas nuevas.")

    procesadas.update(nombres_ok)
    guardar_procesadas(procesadas)

    print(f"\n=== Resumen ===")
    print(f"  Extraídas:   {procesadas_cnt}")
    print(f"  Descartadas: {descartadas}")
    print(f"  Errores:     {errores}")

if __name__ == "__main__":
    main()
