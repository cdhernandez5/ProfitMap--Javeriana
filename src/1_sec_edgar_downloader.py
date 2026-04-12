#!/usr/bin/env python3
"""
=============================================================================
SEC EDGAR - Descarga Trimestral de Estados Financieros (DERA)
=============================================================================

Descarga los Financial Statement Data Sets de la SEC, trimestre por trimestre. Convierte todo a CSV limpio.

¿Qué contiene cada trimestre?
  - sub.csv → Info de cada filing (empresa, CIK, SIC, tipo, fecha)
  - num.csv → TODOS los valores numéricos (ingresos, activos, deuda, etc.)
  - tag.csv → Definiciones de cada tag/variable XBRL
  - pre.csv → Cómo se presentan los datos en los estados financieros

Estimación de tamaño:
  ┌─────────────────────────────────────────────────────────┐
  │  ZIP comprimido por trimestre:     ~40-80 MB            │
  │  CSV descomprimido por trimestre:  ~200-500 MB          │
  │  Total 2015Q1→2025Q4 (40 ZIPs):   ~2.5 GB comprimido   │
  │  Total CSVs descomprimidos:        ~12-18 GB            │
  │  Filings por trimestre:            ~6,000-8,000         │
  │  Datos numéricos por trimestre:    ~500K-2M filas       │
  │  Empresas únicas totales:          ~10,000-12,000       │
  │  Total data points (2015-2025):    ~120-150 millones    │
  └─────────────────────────────────────────────────────────┘

Uso:
  python src/1_sec_edgar_downloader.py

  # O personalizar:
  python src/1_sec_edgar_downloader.py --inicio 2020Q1 --fin 2025Q4
  python src/1_sec_edgar_downloader.py --inicio 2015Q1 --fin 2015Q4 --solo-descargar

Requisitos:
  pip install requests pandas tqdm
=============================================================================
"""

import os
import sys
import time
import zipfile
import argparse
import io
from pathlib import Path
from datetime import datetime

try:
    import requests
    import pandas as pd
    from tqdm import tqdm
except ImportError:
    print("⏳ Instalando dependencias: requests, pandas, tqdm...")
    os.system("pip install requests pandas tqdm -q")
    import requests
    import pandas as pd
    from tqdm import tqdm


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN                                                            ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

# La SEC REQUIERE nombre y email
NOMBRE_EMPRESA = "Javeriana"
EMAIL_CONTACTO = "hechristian@javeriana.edu.co"

# Directorio donde se guardarán los datos
DIRECTORIO_SALIDA = "datos_sec_edgar"

# Pausa entre requests (SEC permite máx 10/s, se usa ~8/s)
PAUSA_ENTRE_REQUESTS = 0.15

BASE_URL = "https://www.sec.gov/files/dera/data/financial-statement-data-sets"
HEADERS = {
    "User-Agent": f"{NOMBRE_EMPRESA} {EMAIL_CONTACTO}",
    "Accept-Encoding": "gzip, deflate",
}

# Archivos dentro de cada ZIP trimestral
ARCHIVOS_DERA = {
    "sub.txt": {
        "descripcion": "Submissions (info de cada filing y empresa)",
        "campos_clave": "adsh, cik, name, sic, form, period, filed, fy, fp, countryba"
    },
    "num.txt": {
        "descripcion": "Valores numéricos (el dato financiero en sí)",
        "campos_clave": "adsh, tag, version, ddate, qtrs, uom, value, segments"
    },
    "tag.txt": {
        "descripcion": "Definiciones de tags XBRL",
        "campos_clave": "tag, version, custom, abstract, datatype, tlabel, doc"
    },
    "pre.txt": {
        "descripcion": "Presentación (líneas de estados financieros)",
        "campos_clave": "adsh, report, line, stmt, tag, plabel"
    },
}


def generar_trimestres(inicio: str, fin: str) -> list:
    """
    Genera lista de trimestres entre inicio y fin.
    Formato: '2015Q1' → [('2015', '1'), ('2015', '2'), ...]
    """
    anio_ini, q_ini = int(inicio[:4]), int(inicio[-1])
    anio_fin, q_fin = int(fin[:4]), int(fin[-1])
    
    trimestres = []
    anio, q = anio_ini, q_ini
    while (anio < anio_fin) or (anio == anio_fin and q <= q_fin):
        trimestres.append((str(anio), str(q)))
        q += 1
        if q > 4:
            q = 1
            anio += 1
    return trimestres


def descargar_trimestre(anio: str, trimestre: str, directorio: Path, 
                         solo_descargar: bool = False) -> dict:
    """
    Descarga un ZIP trimestral y lo convierte a CSVs.
    
    Retorna dict con info del trimestre descargado.
    """
    nombre_zip = f"{anio}q{trimestre}.zip"
    url = f"{BASE_URL}/{nombre_zip}"
    
    dir_trimestre = directorio / f"{anio}Q{trimestre}"
    zip_path = directorio / "zips" / nombre_zip
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    
    resultado = {
        "trimestre": f"{anio}Q{trimestre}",
        "estado": "pendiente",
        "tamano_zip_mb": 0,
        "archivos": {},
    }
    
    # Ya procesado?
    if dir_trimestre.exists() and any(dir_trimestre.glob("*.csv")):
        csvs = list(dir_trimestre.glob("*.csv"))
        total_size = sum(f.stat().st_size for f in csvs)
        print(f"  ✅ {anio}Q{trimestre} ya existe ({total_size/1024/1024:.1f} MB en {len(csvs)} CSVs)")
        resultado["estado"] = "ya_existia"
        return resultado
    
    # Descargar ZIP
    print(f"\n  ⬇  Descargando {anio}Q{trimestre}...", end=" ", flush=True)
    time.sleep(PAUSA_ENTRE_REQUESTS)
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=300, stream=True)
        
        if resp.status_code == 404:
            print("❌ No disponible (probablemente aún no publicado)")
            resultado["estado"] = "no_disponible"
            return resultado
        
        resp.raise_for_status()
        
        # Descargar con barra de progreso
        total = int(resp.headers.get('content-length', 0))
        contenido = bytearray()
        
        with tqdm(total=total, unit='B', unit_scale=True, 
                  desc=f"     {nombre_zip}", leave=False) as barra:
            for chunk in resp.iter_content(chunk_size=256*1024):
                contenido.extend(chunk)
                barra.update(len(chunk))
        
        # Guardar ZIP
        zip_path.write_bytes(bytes(contenido))
        tamano_mb = len(contenido) / (1024*1024)
        resultado["tamano_zip_mb"] = round(tamano_mb, 1)
        print(f"✓ ({tamano_mb:.1f} MB)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        resultado["estado"] = "error"
        return resultado
    
    if solo_descargar:
        resultado["estado"] = "zip_descargado"
        return resultado
    
    # Extraer y convertir TSV → CSV
    dir_trimestre.mkdir(parents=True, exist_ok=True)
    
    print(f"     📂 Extrayendo a CSV:")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            archivos_zip = zf.namelist()
            
            for nombre_archivo in archivos_zip:
                if not nombre_archivo.endswith('.txt'):
                    continue
                
                info = ARCHIVOS_DERA.get(nombre_archivo, {})
                desc = info.get("descripcion", nombre_archivo)
                
                print(f"        → {nombre_archivo:10s}", end=" ", flush=True)
                
                try:
                    with zf.open(nombre_archivo) as f:
                        # Leer TSV (tab-separated) → DataFrame
                        df = pd.read_csv(
                            f, 
                            sep='\t',
                            low_memory=False,
                            encoding='utf-8',
                            on_bad_lines='skip'
                        )
                        
                        # Guardar como CSV
                        nombre_csv = nombre_archivo.replace('.txt', '.csv')
                        ruta_csv = dir_trimestre / nombre_csv
                        df.to_csv(ruta_csv, index=False)
                        
                        tamano_csv = ruta_csv.stat().st_size / (1024*1024)
                        
                        print(f"→ {nombre_csv:10s} "
                              f"({len(df):>9,} filas × {len(df.columns):2d} cols, "
                              f"{tamano_csv:>6.1f} MB) "
                              f"[{desc}]")
                        
                        resultado["archivos"][nombre_csv] = {
                            "filas": len(df),
                            "columnas": list(df.columns),
                            "tamano_mb": round(tamano_csv, 1),
                        }
                        
                except Exception as e:
                    print(f"⚠ Error: {e}")
                    
    except zipfile.BadZipFile:
        print(f"     ⚠ ZIP corrupto, eliminando...")
        zip_path.unlink(missing_ok=True)
        resultado["estado"] = "zip_corrupto"
        return resultado
    
    resultado["estado"] = "completado"
    return resultado


def mostrar_resumen(resultados: list, directorio: Path):
    """Muestra resumen final de la descarga."""
    
    print("\n" + "═"*75)
    print("  📊 RESUMEN DE DESCARGA")
    print("═"*75)
    
    completados = [r for r in resultados if r["estado"] in ("completado", "ya_existia")]
    fallidos = [r for r in resultados if r["estado"] in ("error", "zip_corrupto")]
    no_disponibles = [r for r in resultados if r["estado"] == "no_disponible"]
    
    print(f"\n  ✅ Trimestres descargados:  {len(completados)}")
    print(f"  ❌ No disponibles:         {len(no_disponibles)}")
    if fallidos:
        print(f"  ⚠  Con errores:           {len(fallidos)}")
    
    # Calcular tamaño total
    total_size = 0
    total_filas_num = 0
    for dir_q in directorio.iterdir():
        if dir_q.is_dir() and dir_q.name != "zips":
            for csv in dir_q.glob("*.csv"):
                total_size += csv.stat().st_size
    
    print(f"\n  📁 Directorio: {directorio.absolute()}")
    print(f"  💾 Tamaño total CSVs: {total_size / (1024**3):.2f} GB")
    
    # Mostrar estructura
    print(f"\n  📂 Estructura de archivos:")
    print(f"  {directorio}/")
    for dir_q in sorted(directorio.iterdir()):
        if dir_q.is_dir() and dir_q.name != "zips":
            csvs = list(dir_q.glob("*.csv"))
            dir_size = sum(f.stat().st_size for f in csvs) / (1024*1024)
            print(f"  ├── {dir_q.name}/ ({dir_size:.0f} MB)")
            for csv in sorted(csvs):
                print(f"  │   ├── {csv.name} ({csv.stat().st_size/1024/1024:.1f} MB)")
    
    print(f"\n  ℹ  Cada carpeta trimestral contiene:")
    print(f"      sub.csv → Quién presentó qué filing (empresa, sector, tipo, fecha)")
    print(f"      num.csv → Los VALORES financieros (activos, ingresos, deuda, etc.)")
    print(f"      tag.csv → Diccionario de variables/tags XBRL")
    print(f"      pre.csv → Cómo se presentan en el estado financiero original")




def main():
    parser = argparse.ArgumentParser(
        description="Descarga datos financieros de SEC EDGAR trimestre a trimestre",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
╔═══════════════════════════════════════════════════════════════════╗
║  EJEMPLOS DE USO                                                 ║
╠═══════════════════════════════════════════════════════════════════╣
║                                                                   ║
║  Todo desde 2015:                                                ║
║  $ python src/1_sec_edgar_downloader.py                          ║
║                                                                   ║
║  Solo un rango específico:                                        ║
║  $ python src/1_sec_edgar_downloader.py --inicio 2020Q1 --fin 2024Q4║
║                                                                   ║
║  Solo descargar ZIPs sin convertir a CSV:                        ║
║  $ python src/1_sec_edgar_downloader.py --solo-descargar         ║
║                                                                   ║
║  Siguiente paso — consolidar datos descargados:                  ║
║  $ python src/2_consolidar_variables_sec.py                      ║
║                                                                   ║
╚═══════════════════════════════════════════════════════════════════╝
        """
    )
    
    parser.add_argument('--inicio', default='2015Q1',
                        help='Trimestre inicial, formato: 2015Q1 (default: 2015Q1)')
    parser.add_argument('--fin', default='2025Q4',
                        help='Trimestre final, formato: 2025Q4 (default: 2025Q4)')
    parser.add_argument('--directorio', default=DIRECTORIO_SALIDA,
                        help=f'Directorio de salida (default: {DIRECTORIO_SALIDA})')
    parser.add_argument('--solo-descargar', action='store_true',
                        help='Solo descargar ZIPs, no convertir a CSV')
    parser.add_argument('--eliminar-zips', action='store_true',
                        help='Eliminar ZIPs después de extraer (ahorra espacio)')
    
    args = parser.parse_args()
    directorio = Path(args.directorio)
    directorio.mkdir(parents=True, exist_ok=True)
    
    # Header
    trimestres = generar_trimestres(args.inicio, args.fin)
    
    print("\n" + "═"*75)
    print("  📥 SEC EDGAR — DESCARGA TRIMESTRAL DE ESTADOS FINANCIEROS")
    print("═"*75)
    print(f"  Período:    {args.inicio} → {args.fin} ({len(trimestres)} trimestres)")
    print(f"  Directorio: {directorio.absolute()}")
    print(f"  URL base:   {BASE_URL}")
    
    # Estimación de tamaño
    est_zip = len(trimestres) * 60  # ~60 MB promedio por ZIP
    est_csv = len(trimestres) * 350  # ~350 MB promedio descomprimido
    print(f"\n  💾 Estimación de tamaño:")
    print(f"     ZIPs comprimidos: ~{est_zip/1024:.1f} GB")
    print(f"     CSVs finales:     ~{est_csv/1024:.1f} GB")
    print(f"     Tiempo estimado:  ~{len(trimestres) * 1.5:.0f} minutos")
    
    print(f"\n  🚀 Iniciando descarga...\n")
    
    resultados = []
    for anio, q in trimestres:
        resultado = descargar_trimestre(
            anio, q, directorio, 
            solo_descargar=args.solo_descargar
        )
        resultados.append(resultado)
        
        # Eliminar ZIP si se pidió
        if args.eliminar_zips and resultado["estado"] == "completado":
            zip_path = directorio / "zips" / f"{anio}q{q}.zip"
            zip_path.unlink(missing_ok=True)
    
    mostrar_resumen(resultados, directorio)
    
    print("\n" + "═"*75)
    print("  ✅ ¡PROCESO COMPLETADO!")
    print("═"*75)
    
    print(f"\n  💡 Tip: Para consolidar los datos descargados, ejecuta:")
    print(f"     python src/2_consolidar_variables_sec.py --inicio {args.inicio} --fin {args.fin}")
    
    print()


if __name__ == "__main__":
    main()