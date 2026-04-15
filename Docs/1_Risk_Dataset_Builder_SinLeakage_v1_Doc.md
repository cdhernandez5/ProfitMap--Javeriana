# Documentación Técnica: `1. Risk_Dataset_Builder_SinLeakage_v1.ipynb`

## 1. Descripción General

Este notebook constituye el componente central del pipeline de preparación de datos para el modelo de riesgo financiero corporativo del proyecto **ProfitMap – Javeriana**. Su función principal es transformar los datos financieros brutos extraídos de los registros EDGAR de la U.S. Securities and Exchange Commission (SEC) — contenidos en `DATASET_MODELO.csv` — en un conjunto de datos consolidado, limpio y matemáticamente robusto, listo para alimentar algoritmos de Machine Learning: `training_dataset_riesgo_trimestral.csv`.

### 1.1 Pregunta Central del Modelo

> *"Dado el estado financiero actual de este proveedor, ¿cuál será su nivel de riesgo el próximo trimestre?"*

### 1.2 Principio Anti-Leakage (Sin Fuga de Datos)

La característica metodológica **más relevante** de este pipeline es su diseño **sin leakage** (sin fuga de datos).  El notebook formula la analítica de forma estrictamente **predictiva**, enmarcando el problema como una serie temporal panelada. Esto significa que:

- Las **features** de entrada del período `t` (RATIO_*, delta, flags) **no comparten relación matemática directa** con el **target** del período `t+1`.
- Los componentes internos del Z-Score (X1–X5) y las partidas crudas del balance que lo alimentan **quedan explícitamente excluidos** como features de entrenamiento.
- El Z-Score se calcula internamente **solo como puente** para construir el `risk_score_0_1` del período actual, el cual se utiliza después como feature autorregresivo (punto en el tiempo `t` prediciendo el tiempo `t+1`).

Este enfoque elimina cabalmente la fuga de datos que surgiría si el target estuviera construido con las mismas variables que se usan para predecirlo.

---

## 2. Flujo del Pipeline

El flujo de transformación completo se resume en el siguiente diagrama:

```
CSV fuente (DATASET_MODELO.csv)
  ↓ (Paso 1) Configuración de entorno
  ↓ (Paso 2) Carga del archivo fuente
  ↓ (Paso 3) Limpieza + Preparación
  ↓ (Paso 4) Anualización de flujos
  ↓ (Paso 5) Cálculo Z-Score + risk_score_0_1  (uso INTERNO — no va como feature)
  ↓ (Paso 6) Features: Flags de alerta financiera
  ↓ (Paso 7) Features: Evolución temporal (delta)
  ↓ (Paso 8) Target: risk_score_0_1 del período t+1  (shift -1 por empresa)
  ↓ (Paso 9) Selección de features (sin ingredientes del Z-Score)
  ↓ (Paso 10) Filtros de calidad y dataset final
  ↓ (Paso 11) Visualizaciones confirmatorias
  ↓ (Paso 12) Exportación del dataset final
  ↓
Dataset final de entrenamiento (training_dataset_riesgo_trimestral.csv)
```

---

## 3. Archivos de Entrada y Salida

| Tipo | Ruta | Descripción |
|------|------|-------------|
| **Entrada** | `../../data_variables_crudas/DATASET_MODELO.csv` | Dataset consolidado con variables crudas de los reportes SEC EDGAR. Contiene ~271,375 filas y 177 columnas para ~11,460 empresas únicas. |
| **Salida** | `../../data_variables_crudas/training_dataset_riesgo_trimestral.csv` | Dataset de entrenamiento limpio y libre de leakage. Contiene ~85,924 filas y 31 columnas para ~5,137 empresas únicas. |

---

## 4. Parámetros de Configuración

| Parámetro | Valor | Descripción |
|-----------|-------|-------------|
| `FORMS_TO_KEEP` | `['10-K', '10-K/A', '10-Q', '10-Q/A']` | Formularios SEC relevantes (informes anuales y trimestrales, incluidas sus enmiendas). |
| `MIN_ASSETS` | `100,000` | Umbral mínimo de activos (USD) para filtrar empresas tributarias de escala insuficiente. |
| `ZSCORE_CLIP` | `(-20, 20)` | Recorte extremo del Z-Score para evitar valores atípicos desmesurados. |
| `ANNUALIZATION` | `{'Q1': 4.0, 'Q2': 2.0, 'Q3': 4/3, 'FY': 1.0, 'Q4': 1.0}` | Factores multiplicadores para anualizar flujos acumulados trimestrales al equivalente anual (*Run-Rate*). |
| `PERIOD_ORDER` | `{'Q1': 1, 'Q2': 2, 'Q3': 3, 'FY': 4, 'Q4': 4}` | Ordenamiento numérico de los períodos fiscales para facilitar el cálculo de deltas secuenciales. |

---

## 5. Explicación Detallada de las Etapas

### Paso 0 — Instalación de Dependencias

Instalación de las librerías necesarias:
- `pandas`, `numpy` — Manipulación de datos
- `matplotlib`, `seaborn` — Visualización
- `scikit-learn` — Utilidades de Machine Learning

### Paso 1 — Imports y Configuración

Se establecen:
- Rutas relativas base para importación (`INPUT_PATH`) y exportación (`OUTPUT_PATH`), garantizando portabilidad.
- Filtros cardinales: formularios SEC a conservar, umbral mínimo de activos ($100K USD), rango de recorte del Z-Score.
- Factores de anualización para homogeneizar flujos de distintos períodos fiscales.
- Orden de los períodos fiscales para cálculos de evolución temporal.

### Paso 2 — Carga del Archivo Fuente

- Lectura del CSV fuente con `pd.read_csv()` en modo `low_memory=False`.
- **Resultado observado**: ~271,375 filas, 177 columnas, 11,460 empresas únicas (CIK).
- Distribución equilibrada por período fiscal: Q1 (~24.8%), Q2 (~25.1%), Q3 (~25.3%), FY (~24.8%).

### Paso 3 — Limpieza y Preparación

Este paso ejecuta múltiples operaciones de depuración:

1. **Conversión de tipos**: Todas las columnas financieras (`FINANCIAL_COLS`) y de ratios (`RATIO_COLS`) se convierten a tipo numérico con `pd.to_numeric(errors='coerce')`.

2. **Consolidación de ingresos**: Se unifica la variable de ingresos que cambió de nombre entre normativas contables. Antes de 2018, los ingresos se reportaban como `Revenues`; a partir de ASC 606, como `RevenueFromContractWithCustomerExcludingAssessedTax`. La columna `revenues_consolidado` toma el primero disponible con fallback al segundo.

3. **Normalización del período fiscal**: Se construye la columna `periodo` como concatenación de año fiscal (`fy`) y período (`fp`), y se crea `fp_orden` como valor numérico ordinal.

4. **Filtrado de formularios**: Solo se conservan los formularios relevantes (10-K, 10-K/A, 10-Q, 10-Q/A), descartando otros tipos de reportes SEC.

5. **Filtrado por activos mínimos**: Se eliminan registros con `Assets < 100,000` USD.

6. **Resolución de duplicados por enmienda**: Cuando una empresa presenta múltiples reportes para el mismo período (por ejemplo, un 10-Q y su enmienda 10-Q/A), se conserva únicamente el más reciente según la fecha de presentación (`filed`).

### Paso 4 — Anualización de Flujos para Comparabilidad Trimestral

**Problema**: Los datos acumulados de los reportes 10-Q no son directamente comparables entre trimestres. Un reporte Q1 refleja 3 meses de operación, Q2 refleja 6 meses acumulados, y Q3 refleja 9 meses acumulados. Solo el reporte anual (FY/Q4) reporta el año completo.

**Solución**: Se aplican factores de anualización (*Run-Rate*) a las variables de flujo para proyectarlas al equivalente anual:

| Período | Factor | Lógica |
|---------|--------|--------|
| Q1 | ×4.0 | 3 meses → 12 meses |
| Q2 | ×2.0 | 6 meses → 12 meses |
| Q3 | ×1.333 | 9 meses → 12 meses |
| FY / Q4 | ×1.0 | Ya es anual, sin ajuste |

**Variables anualizadas** (sufijo `_anual`):
- `OperatingIncomeLoss`
- `revenues_consolidado`
- `NetIncomeLoss`
- `GrossProfit`
- `InterestExpense`
- `NetCashProvidedByUsedInOperatingActivities`

### Paso 5 — Cálculo del Z-Score (Uso INTERNO)

> [!IMPORTANT]
> El Z-Score y sus componentes X1–X5 se calculan aquí **solo para construir el target**. Estas columnas **NO son incluidas como features** de entrenamiento porque son exactamente los ingredientes de la variable objetivo (data leakage).

Se implementa la fórmula del **Altman Z-Score** modificado (versión para empresas privadas/no manufactureras):

```
Z = 0.717 × X1 + 0.847 × X2 + 3.107 × X3 + 0.420 × X4 + 0.998 × X5
```

Donde:
| Componente | Fórmula | Significado |
|------------|---------|-------------|
| **X1** (`X1_wc_assets`) | (Activos Corrientes − Pasivos Corrientes) / Activos Totales | Capital de trabajo / Activos — Liquidez |
| **X2** (`X2_re_assets`) | Utilidades Retenidas / Activos Totales | Rentabilidad acumulada |
| **X3** (`X3_ebit_assets`) | EBIT Anualizado / Activos Totales | Productividad operativa |
| **X4** (`X4_equity_liab`) | Patrimonio Neto / Pasivos Totales | Estructura de capital |
| **X5** (`X5_rev_assets`) | Ingresos Anualizados / Activos Totales | Eficiencia de uso de activos |

> [!NOTE]
> Se utiliza un `EPS = 1e-9` en los denominadores para evitar divisiones por cero.

El Z-Score se recorta al rango `[-20, 20]` para evitar valores extremos.

**Transformación a `risk_score_0_1`**: Se aplica una función sigmoide logística centrada en 1.23 (umbral clásico de distress del Z-Score):

```python
risk_score_0_1 = 1 / (1 + exp(altman_zscore - 1.23))
```

Esto produce un score entre 0 (seguro) y 1 (máximo riesgo).

**Resultado observado**: Z-Score calculado para 88,118 registros (32.5% del dataset).

### Paso 6 — Features de Señales de Alerta Financiera

Se construyen **cinco flags binarias** independientes del Z-Score, que capturan señales cualitativas de deterioro financiero:

| Flag | Fórmula | Significado | Prevalencia |
|------|---------|-------------|-------------|
| `flag_patrimonio_negativo` | `StockholdersEquity < 0` | Patrimonio neto negativo (vaciado técnico) | 14.8% |
| `flag_perdida_neta` | `NetIncomeLoss < 0` | La empresa reporta pérdidas | 43.5% |
| `flag_deficit_acumulado` | `RetainedEarningsAccumulatedDeficit < 0` | Utilidades retenidas negativas históricamente | 52.3% |
| `flag_liquidez_critica` | `LiabilitiesCurrent > AssetsCurrent` | Pasivos corrientes superan activos corrientes | 20.5% |
| `flag_fco_negativo` | `NetCashProvidedByUsedInOperatingActivities < 0` | Flujo de caja operativo negativo | 36.0% |

Adicionalmente, `n_alertas` computa la suma de todas las alertas activas (de 0 a 5), funcionando como un umbral agregado de crisis simultáneas.

### Paso 7 — Features de Evolución Temporal (Deltas)

Se descompone la dinámica de riesgo inter-trimestral en derivadas sucesivas:

| Variable | Fórmula | Significado |
|----------|---------|-------------|
| `risk_score_prev` | `shift(1)` por empresa | Score del período anterior (t-1) — feature autorregresivo |
| `delta_risk_score` | `risk_score_0_1 - risk_score_prev` | Primera derivada: cambio de riesgo entre t y t-1 |
| `flag_deterioro` | `delta_risk_score > 0` | Indicador binario: 1 si el riesgo empeoró |
| `delta_risk_score_prev` | `shift(1)` del delta | Delta del período previo |
| `aceleracion_riesgo` | `delta_risk_score - delta_risk_score_prev` | Segunda derivada: aceleración/desaceleración del riesgo |

**Resultado observado**:
- Registros con delta calculado: 80,903
- Riesgo empeoró: 49.8%
- Riesgo mejoró: 45.0%

### Paso 8 — Construcción del Target Futuro (Sin Leakage)

> [!IMPORTANT]
> Este es el cambio central respecto a versiones anteriores del pipeline. El target es el `risk_score_0_1` del **período siguiente** de la misma empresa, convirtiendo el problema en genuinamente predictivo.

Se construyen tres variantes de variable objetivo:

| Target | Tipo | Descripción |
|--------|------|-------------|
| **`risk_score_next`** | Continuo [0, 1] | ⭐ **RECOMENDADO** — Score de riesgo del período `t+1`. Para modelos de regresión. |
| **`target_distress_next`** | Binario {0, 1} | `1` si `risk_score_next ≥ 0.5`. Clasificación binaria: SAFE/GREY vs DISTRESS. |
| **`target_class_next`** | Multiclase {0, 1, 2} | Clasificación de tres clases: 0=SAFE (<0.35), 1=GREY (0.35–0.65), 2=DISTRESS (≥0.65). |

La operación clave es `shift(-1)` agrupado por `cik` (empresa), que desplaza el score actual una posición hacia adelante en la serie temporal de cada empresa.

**Resultado observado**:
- Registros con target calculado: 85,924 (31.7%)
- Distribución del `target_distress_next`: Clase 0 (SAFE/GREY): 40.3% | Clase 1 (DISTRESS): 59.7%

### Paso 9 — Selección de Features (Sin Ingredientes del Z-Score)

Se implementa la **segregación funcional** que da nombre a la versión "SinLeakage". La tabla siguiente documenta cada grupo de variables y su admisibilidad como feature:

| Grupo | ¿Permitido? | Razón |
|-------|-------------|-------|
| `RATIO_*` (10 ratios) | ✅ Sí | Ratios financieros independientes; período actual predice el futuro |
| `risk_score_0_1` | ✅ Sí | Feature autorregresivo (período `t` → `t+1`) |
| `delta_risk_score`, derivadas | ✅ Sí | Tendencia reciente, no constituye leakage |
| `flag_*`, `n_alertas` | ✅ Sí | Señales binarias independientes |
| `fp_orden` | ✅ Sí | Contexto temporal (trimestre actual) |
| `X1–X5`, `altman_zscore` | ❌ No | Ingredientes directos del target |
| `Assets`, `OperatingIncomeLoss`, etc. | ❌ No | Partidas crudas usadas en el Z-Score |
| `*_anual`, `factor_anual` | ❌ No | Versiones derivadas de las partidas crudas |

**Grupos de features finales**:

- **Identificadores** (metadatos, no para entrenamiento): `adsh`, `cik`, `name`, `sic`, `form`, `period`, `filed`, `fy`, `fp`, `periodo`, `fp_orden`, `countryba`, `stprba`
- **Ratios financieros**: `RATIO_apalancamiento`, `RATIO_liquidez_corriente`, `RATIO_deuda_equity`, `RATIO_margen_operativo`, `RATIO_margen_neto`, `RATIO_cobertura_intereses`, `RATIO_cash`, `RATIO_ROA`, `RATIO_ROE`, `RATIO_cashflow_deuda`
- **Temporales**: `risk_score_0_1`, `risk_score_prev`, `delta_risk_score`, `delta_risk_score_prev`, `aceleracion_riesgo`
- **Alertas**: `flag_patrimonio_negativo`, `flag_perdida_neta`, `flag_deficit_acumulado`, `flag_liquidez_critica`, `flag_fco_negativo`, `n_alertas`, `flag_deterioro`
- **Contexto**: `fp_orden`

> [!WARNING]
> En la ejecución observada, las 10 columnas `RATIO_*` no fueron encontradas en el dataset de entrada. Esto sugiere que estas variables no están presentes en `DATASET_MODELO.csv` y requieren generación previa o en una fase complementaria del pipeline.

### Paso 10 — Filtros de Calidad y Dataset Final

Se aplica un único filtro crítico: **solo se conservan registros que tienen target calculado** (`risk_score_next` no nulo). Los registros del último período disponible de cada empresa se pierden porque no existe "período siguiente" para construir el target.

**Resultado observado**:
- Registros antes: 271,375
- Con target calculado: 85,924 (reducción de ~185,451 registros)
- Empresas únicas: 5,137

**Completitud de features en el dataset final**:

| Feature | Completitud | Estado |
|---------|-------------|--------|
| `flag_*` (5 flags) y `n_alertas` | 100.0% | ✅ |
| `fp_orden` | 100.0% | ✅ |
| `risk_score_0_1` | 94.2% | ✅ |
| `risk_score_prev` | 88.1% | ✅ |
| `delta_risk_score` | 87.1% | ✅ |
| `flag_deterioro` | 87.1% | ✅ |
| `delta_risk_score_prev` | 81.5% | ✅ |
| `aceleracion_riesgo` | 80.7% | ✅ |

### Paso 11 — Visualizaciones

Se generan gráficas analíticas exploratorias para confirmar la validez del dataset construido:
- **Mapa de correlación de Pearson** (triangular inferior): Verifica las relaciones entre features y ausencia de multicolinealidad directa con el target `t+1`.
- **Diagramas de distribución**: Corroboran la distribución categórica del target y la representatividad de las clases.

### Paso 12 — Exportación del Dataset Final

Se seleccionan las columnas finales organizadas en cinco grupos:

| Grupo | Cantidad | Descripción |
|-------|----------|-------------|
| Identificadores | 13 | Metadatos (CIK, nombre, sector, fechas, etc.) |
| Features RATIO_* | 0 | No disponibles en esta ejecución |
| Features temporales | 5 | Score autorregresivo + deltas + aceleración |
| Features flag_* | 7 | Alertas binarias + conteo agregado + deterioro |
| Referencia interna | 4 | `risk_score_0_1`, `altman_zscore`, targets binario/multiclase |
| **TARGET principal** | 1 | `risk_score_next` |
| **Total** | **31** | — |

El dataset final se exporta a `training_dataset_riesgo_trimestral.csv`.

---

## 6. Consideraciones Técnicas y Limitaciones

### 6.1 Variables RATIO_* Faltantes

Las 10 variables de ratios financieros (`RATIO_apalancamiento`, `RATIO_liquidez_corriente`, etc.) no están presentes en el `DATASET_MODELO.csv` utilizado. Esto implica que el dataset de entrenamiento resultante opera con un subconjunto reducido de features (13 features efectivas vs las 23 previstas). **Se recomienda verificar si estas variables deben ser generadas en un paso previo del pipeline** o si provienen de otra fuente de datos.

### 6.2 Pérdida de Registros por Shift Temporal

La operación `shift(-1)` por empresa elimina necesariamente el último registro de cada empresa (no tiene "período siguiente"). Esto reduce el dataset de ~271K a ~86K registros. **Esta reducción es estructural e inherente al enfoque predictivo** adoptado.

### 6.3 Completitud No Uniforme

Las features temporales (derivadas, score previo, aceleración) tienen completitud variable (80.7% – 94.2%) porque requieren existencia de períodos anteriores para ser calculadas. Los primeros registros de cada empresa tendrán valores nulos en estas métricas.

### 6.4 Desbalance del Target

El target binario `target_distress_next` muestra desbalance moderado (59.7% distress vs 40.3% safe/grey). Dependiendo del algoritmo de ML elegido, puede ser necesario aplicar técnicas de balanceo (SMOTE, undersampling, pesos de clase, etc.).

---

## 7. Dependencias

| Librería | Uso |
|----------|-----|
| `pandas` | Manipulación de DataFrames, lectura/escritura CSV |
| `numpy` | Operaciones numéricas, función sigmoide, NaN handling |
| `matplotlib` | Gráficos de distribución |
| `seaborn` | Mapas de correlación (heatmaps) |
| `scikit-learn` | Utilidades auxiliares de ML |

---

## 8. Resumen del Pipeline de Transformación

```
┌─────────────────────────────────────────────────────────────┐
│                    DATOS CRUDOS SEC EDGAR                    │
│              271,375 filas × 177 columnas                   │
│                   11,460 empresas                           │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│             LIMPIEZA + PREPARACIÓN (Paso 3)                 │
│  • Conversión de tipos numéricos                            │
│  • Consolidación Revenues (pre/post ASC 606)                │
│  • Filtro: forms 10-K/10-Q + enmiendas                     │
│  • Filtro: Assets ≥ $100K                                   │
│  • Deduplicación: conservar reporte más reciente            │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│          ANUALIZACIÓN DE FLUJOS (Paso 4)                    │
│  • Run-rate de Q1(×4), Q2(×2), Q3(×1.33), FY(×1)          │
│  • 6 variables de flujo anualizadas                         │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│       Z-SCORE INTERNO + RISK SCORE (Paso 5)                 │
│  • Altman Z-Score (X1–X5), clip [-20, 20]                   │
│  • risk_score_0_1 via sigmoide (centro: 1.23)               │
│  ⚠ USO INTERNO — NO va como feature                        │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│      FEATURE ENGINEERING (Pasos 6-7)                        │
│  • 5 flags binarias de alerta + n_alertas                   │
│  • risk_score_prev, delta, aceleración, flag_deterioro      │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│        CONSTRUCCIÓN TARGET FUTURO (Paso 8)                  │
│  • risk_score_next = shift(-1) por empresa                  │
│  • target_distress_next (binario)                           │
│  • target_class_next (3 clases)                             │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│    SELECCIÓN + FILTRADO + EXPORTACIÓN (Pasos 9-12)          │
│  • Exclusión de X1-X5, partidas crudas, anualizadas         │
│  • Solo registros con target calculado                      │
│  • Visualización confirmatoria                              │
│  • Exportación CSV final                                    │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              DATASET DE ENTRENAMIENTO                       │
│              85,924 filas × 31 columnas                     │
│                   5,137 empresas                            │
│              TARGET: risk_score_next                        │
│              FEATURES: 13 variables efectivas               │
│              ❌ SIN LEAKAGE                                 │
└─────────────────────────────────────────────────────────────┘
```
