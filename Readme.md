# Proyecto de Modelamiento de Riesgo Financiero (SEC DERA)

Este proyecto tiene como objetivo construir un pipeline de datos y un modelo predictivo de riesgo financiero (riesgo de quiebra, distress financiero, etc.) utilizando datos públicos de estados financieros extraídos del sistema EDGAR de la Securities and Exchange Commission (SEC) de Estados Unidos.

----

## 1. Descarga de Datos Crudos

El primer paso necesario para reproducir el pipeline es la obtención de la data de la SEC. Para ello se ha desarrollado el script `src/1_sec_edgar_downloader.py`.

### ¿Qué hace este script?
El script `1_sec_edgar_downloader.py` se encarga **exclusivamente** de automatizar la descarga masiva de los "Financial Statement Data Sets" desde la SEC trimestre a trimestre (desde 2015Q1 en adelante).

Descarga los archivos ZIP trimestrales y los procesa para extraer archivos CSV clave que contienen la materia prima del proyecto:
- **`sub.csv`**: Información de cada filing o presentación (nombre de la empresa, código CIK, industria SIC, tipo de formulario 10-K/10-Q, y fechas).
- **`num.csv`**: Todos los valores numéricos brutos reportados (ingresos, activos totales, pasivos, deuda, etc.).
- **`tag.csv`**: Definiciones y catálogos de cada métrica/tag en el estándar XBRL.
- **`pre.csv`**: Información de presentación jerárquica en los estados financieros.

### Ejecución del primer paso
> [!IMPORTANT]
> **Carpeta de Descarga:** Al ejecutar el script, descargará automáticamente toda la data en una carpeta llamada **`datos_sec_edgar`**. Esta carpeta se creará exactamente en la ruta desde la cual ejecutes el script. Ten en cuenta que la descarga completa de varios años puede llegar a ocupar entre 12 y 18 GB en el disco.

Para iniciar la descarga, asegúrate de estar en la raíz de tu proyecto e instala los requisitos si no los tienes:
```bash
pip install requests pandas tqdm
```

Y ejecuta el script principal:
```bash
python src/1_sec_edgar_downloader.py
```

*(Puedes consultar más opciones de ejecución dentro del archivo o usando `python src/1_sec_edgar_downloader.py --help`)*

---

## 2. Consolidación de Datos Crudos

El segundo paso en el flujo de trabajo es unificar la información descargada y estructurarla en un formato adecuado para su análisis, usando el script `2_consolidar_variables_sec.py`.

### ¿Qué hace este paso?
Este script recorre todas las carpetas trimestrales descargadas previamente en `datos_sec_edgar/` y consolida **solo la data numérica pura** en un único registro por empresa y fecha (`VARIABLES_FINANCIERAS_CRUDAS.csv`). 

> [!NOTE]
> **Dataset Histórico:** Para efectos de este proyecto, el resultado consolidado proveniente de una ejecución realizada el **3 de abril** de este flujo fue copiado explícitamente de forma manual a la carpeta **`data_variables_crudas/`**. Desde este directorio es de donde se consumirán los datos para las siguientes fases del modelado.

Aplica múltiples filtros de calidad cruciales, como descartar filas con segmentos parciales para quedarse con datos consolidados, ignorar empresas subsidiarias (co-registrants), filtrar correctamente las unidades de medida (USD para valores y 'shares' para de acciones) y priorizar los datos usando una lógica inteligente (max qtrs). Su objetivo no es calcular ratios ni variables nuevas, sino garantizar que la "materia prima" sea limpia y representativa de cada filing.

### Documentación Detallada
Para una explicación matemática y técnica mucho más exhaustiva sobre los filtros y todas las variables XBRL consolidadas en esta fase, consulta revisar la documentación oficial: 
📄 **[Docs/01_consolidador_variables_crudas.md](Docs/01_consolidador_variables_crudas.md)**

### Ejecución y Parámetros

| Parámetro | Descripción | Valor por defecto |
|-----------|-------------|-------------------|
| `--inicio` | Trimestre inicial del rango a consolidar | `2014Q1` |
| `--fin` | Trimestre final del rango a consolidar | `2025Q4` |
| `--directorio` | Carpeta con los datos descargados por el paso 1 | `datos_sec_edgar` |
| `--salida` | Ruta del CSV de salida | `{directorio}/VARIABLES_FINANCIERAS_CRUDAS.csv` |

```bash
# Usar todos los valores por defecto (2014Q1 → 2025Q4)
python src/2_consolidar_variables_sec.py

# Consolidar solo un rango específico
python src/2_consolidar_variables_sec.py --inicio 2020Q1 --fin 2024Q4

# Especificar directorio y archivo de salida
python src/2_consolidar_variables_sec.py --directorio datos_sec_edgar --salida mi_dataset.csv
```

---

## 3. Validación, Feature Engineering y Preparación del Dataset

El tercer paso unifica la limpieza final, la creación de nuevas variables analíticas (Feature Engineering) y la optimización del dataset en una sola ejecución. Toma la "materia prima" del paso dos y la convierte en el dataset definitivo para entrenar los modelos numéricos de riesgo.

### Ejecución y Parámetros
El comando exacto que se empleó históricamente para procesar los datos de este proyecto fue el siguiente:

```bash
python src/3_validacion_features.py --entrada datos_sec_edgar/VARIABLES_FINANCIERAS_CRUDAS.csv --salida datos_sec_edgar/DATASET_MODELO_LISTO.csv --min-cobertura 15 --min-assets 100000 --winsor-lower 2.0 --winsor-upper 98.0
```

**Explicación de los parámetros utilizados:**
- `--entrada`: Ruta de nuestro dataset de entrada.
- `--salida`: Ruta final con el dataset pulido que alimentará al modelo.
- `--min-cobertura 15`: Exige que cualquier nueva variable derivada (por ejemplo ratios financieros) cuente con al menos un 15% de completitud estadística. Desecha columnas vacías (llenas de NaNs).
- `--min-assets 100000`: Conserva solo las empresas con activos totales superiores a 100,000 USD. Resulta un filtro decisivo para **eliminar shell companies (empresas cascarón)** u operaciones residuales en SEC, que de otro modo generarían saltos matemáticos absurdos (como divisiones por cero o cercanas a cero).

**Winsorización (El control de Outliers):**
- `--winsor-lower 2.0` y `--winsor-upper 98.0`: La *Winsorización* limita numéricamente los valores anormales. Cualquier dato financiero por debajo del percentil 2% o por encima del 98% es ajustado y "suavizado" matemáticamente hacia el nivel exacto de ese percentil (evitando la pérdida total de la fila temporal).
  - *¿Por qué son importantes y por qué 2 y 98?* Los formularios de la SEC (XBRL) a menudo contienen errores humanos de digitación o situaciones de negocio tan anormales que reportan miles de porciento en márgenes. Si alimentas a un modelo de *Machine Learning* con estos valores atípicos salvajes, arruinas sus pesos. Se eligió el percentil amplio de **2 y 98** porque un universo de datos de compañías tan disparejas en EDGAR tiene demasiada varianza natural como para usar un rígido (1-99). El 2-98 limpia el verdadero "ruido" nocivo preservando dinámicas reales de la empresa.

### Documentación Detallada
Para estudiar más a fondo las reglas precisas y casi las 73 características (features/ratios) que se construyen y calculan durante este paso, revisa este último documento técnico:
📄 **[Docs/03_pipeline_validacion_y_features.md](Docs/03_pipeline_validacion_y_features.md)**
