# 🔍 Hallazgos del Análisis Exploratorio — VARIABLES_FINANCIERAS_CRUDAS.csv

**Fecha:** 3 de abril de 2026  
**Archivo analizado:** `datos_sec_edgar/VARIABLES_FINANCIERAS_CRUDAS.csv` (222 MB)

---

## 1. Dimensiones

| Métrica | Valor |
|---------|-------|
| **Filas** | 305,209 |
| **Columnas** | 196 |
| **Empresas únicas (CIK)** | 12,679 |
| **Columnas metadata** | 12 |
| **Tags financieros** | 92 |
| **Columnas `_qtrs`** | 92 |

## 2. Distribución de Formularios

| Formulario | Cantidad | % |
|-----------|----------|---|
| 10-Q | 222,450 | 72.9% |
| 10-K | 72,759 | 23.8% |
| 10-Q/A | 5,993 | 2.0% |
| 10-K/A | 4,007 | 1.3% |

> [!NOTE]
> La distribución es coherente: ~75% trimestrales, ~25% anuales. Los amendments (/A) son ~3.3% del total.

## 3. Distribución de Períodos Fiscales

| Período | Cantidad | % |
|---------|----------|---|
| FY | 76,765 | 25.2% |
| Q3 | 76,353 | 25.0% |
| Q1 | 76,082 | 24.9% |
| Q2 | 76,002 | 24.9% |
| **Q4** | **6** | **0.0%** |

> [!WARNING]
> **Solo 6 registros con `fp=Q4`.** Esto confirma lo observado en el notebook. En la taxonomía SEC, `fp=FY` corresponde al reporte anual completo, que normalmente cubre el Q4. Las empresas no reportan un "Q4" separado en un 10-K — el 10-K ya ES el reporte del cuarto trimestre. Los 6 registros Q4 son anomalías que deben investigarse.

## 4. Rango Temporal

| Campo | Mínimo | Máximo |
|-------|--------|--------|
| `fy` (año fiscal) | 2004 | 2027 |
| `period` | 20041231 | 20251130 |

> [!WARNING]
> **Años fiscales 2004-2013 y 2026-2027** son sorprendentes dado que el script consolida datos de 2014Q1 a 2025Q4. Esto ocurre porque:
> - **fy < 2014:** Empresas con año fiscal no estándar que presentaron reportes en trimestres dentro del rango 2014-2025 pero con `fy` anterior (ej: empresa con FYE en marzo que reporta en Q1-2014 pero su `fy=2013`).
> - **fy > 2025:** Empresas con año fiscal adelantado que presentaron en 2025 pero su `fy=2026` o `2027`.
> 
> **Acción recomendada:** Estos registros son técnicamente válidos pero pueden distorsionar análisis temporales. Evaluar si filtrarlos.

## 5. Duplicados

| Tipo | Cantidad |
|------|----------|
| `adsh` duplicados | **0** ✅ |
| Filas con `cik+fy+fp` duplicado | **17,042** (en 8,145 grupos) |

> [!IMPORTANT]
> **0 duplicados por `adsh`** confirma que el script de consolidación funciona correctamente — cada filing es único.
> 
> **17,042 filas con `cik+fy+fp` duplicado** es esperado y NO es un error. Ocurre cuando:
> - Una empresa presenta un 10-K y luego un 10-K/A para el mismo período → ambos tienen el mismo `cik+fy+fp`
> - Una empresa presenta un 10-Q y luego un 10-Q/A
> 
> **Acción recomendada:** En la limpieza, quedarse con el filing más reciente (mayor `filed`) para cada `cik+fy+fp`, ya que el amendment (/A) siempre contiene la versión corregida.

## 6. Integridad de Assets

| Condición | Cantidad |
|-----------|----------|
| Assets < 0 | **6** |
| Assets = 0 | **2,584** |
| Assets NaN | **4,957** |

> [!WARNING]
> **6 filas con Assets negativos** son errores de datos (los activos nunca son negativos). Deben eliminarse.
> 
> **2,584 filas con Assets = 0** son probables shell companies o empresas pre-operativas. Deben eliminarse para modelamiento de riesgo.
> 
> **4,957 filas sin Assets** pierden utilidad para la mayoría de ratios financieros. Marcar para evaluación.

## 7. Rangos de Valores Clave

| Variable | Min | Max | Válidos | Negativos |
|----------|-----|-----|---------|-----------|
| Assets | -12,673,073 | 4,560,205,000,000 | 300,252 | **6** |
| Liabilities | -14,894,706 | 4,255,655,000,000 | 249,127 | **73** |
| StockholdersEquity | -23,552,000,000 | 698,155,000,000 | 278,177 | **62,157** |
| Revenues | -3,388,551,000 | 680,985,000,000 | 108,996 | **300** |
| NetIncomeLoss | -49,746,000,000 | 112,010,000,000 | 280,107 | 146,216 |
| OperatingIncomeLoss | -20,727,000,000 | 133,050,000,000 | 228,993 | 127,963 |
| RetainedEarnings | -133,805,000,000 | 743,987,000,000 | 278,378 | 168,401 |
| Cash | -11,225,535 | 419,097,000,000 | 255,102 | **66** |
| SharesOutstanding | -891,624,558 | 513,773,072,000,000 | 216,984 | **4** |

### Observaciones:

- **StockholdersEquity negativo en 62,157 filas (22.3%):** Esto es normal en contabilidad. Empresas como Starbucks, McDonald's y muchas tech tienen equity negativo por recompras masivas de acciones. NO es un error.
- **NetIncomeLoss negativo en 146,216 filas (52.2%):** Normal — más de la mitad de las empresas reportan pérdidas en algún período. NO es un error.
- **Revenues negativo: 300 filas** — Probables errores o ajustes contables. Deberían investigarse.
- **Cash negativo: 66 filas** — Errores de datos. El efectivo no puede ser negativo. Convertir a NaN.
- **SharesOutstanding negativo: 4 filas** — Errores. Convertir a NaN.
- **SharesOutstanding max = 513,773,072,000,000**: Outlier extremo. Verificar si es un error de escala.

## 8. Cobertura por Variable (TOP 30)

| Tag | Válidos | Cobertura |
|-----|---------|-----------|
| Assets | 300,252 | 98.4% |
| NetIncomeLoss | 280,107 | 91.8% |
| RetainedEarnings | 278,378 | 91.2% |
| StockholdersEquity | 278,177 | 91.1% |
| CashAndCashEquivalents | 255,102 | 83.6% |
| NetCashProvidedByUsedInOperatingActivities | 253,655 | 83.1% |
| Liabilities | 249,127 | 81.6% |
| NetCashProvidedByUsedInFinancingActivities | 245,369 | 80.4% |
| CommonStockValue | 241,839 | 79.2% |
| LiabilitiesCurrent | 236,644 | 77.5% |
| AssetsCurrent | 236,635 | 77.5% |
| OperatingIncomeLoss | 228,993 | 75.0% |
| CommonStockSharesOutstanding | 216,984 | 71.1% |
| PropertyPlantAndEquipmentNet | 216,928 | 71.1% |
| IncomeTaxExpenseBenefit | 203,892 | 66.8% |
| ShareBasedCompensation | 195,556 | 64.1% |
| EarningsPerShareBasic | 187,031 | 61.3% |
| InterestExpense | 154,331 | 50.6% |

### Tags con cobertura < 10%:

| Tag | Cobertura |
|-----|-----------|
| IncomeLossFromContinuingOperations | 7.7% |
| ShortTermInvestments | 7.4% |
| ShortTermBorrowings | 7.2% |
| RestrictedCash | 6.5% |
| AssetsNoncurrent | 6.5% |
| LongTermDebt | 5.8% |
| DeferredRevenueNoncurrent | 5.3% |
| RepaymentsOfDebt | 4.8% |
| MarketableSecuritiesCurrent | 4.1% |
| DebtCurrent | 4.1% |
| ProceedsFromIssuanceOfDebt | 4.0% |
| InterestExpenseDebt | 3.6% |

> [!NOTE]
> Baja cobertura **no significa que sean malas variables**. Simplemente no todas las empresas reportan estos tags. Por ejemplo, `ShortTermBorrowings` solo aplica a empresas endeudadas, y `MarketableSecuritiesCurrent` a empresas con inversiones financieras.

## 9. Distribución de `qtrs` (Cobertura Temporal)

### Assets_qtrs:
- **qtrs=0: 301,664** (100% de los registros con Assets)
- ✅ Correcto: Assets es dato puntual (balance al cierre), por lo que `qtrs=0`.

### Revenues_qtrs (principales):
- qtrs=1: 28,678 (un trimestre)
- qtrs=2: 28,646 (dos trimestres acumulados)
- qtrs=3: 28,671 (tres trimestres acumulados)
- qtrs=4: 29,342 (año completo)
- qtrs=0: 31 (dato puntual — **anómalo para Revenues**)
- qtrs ≥ 5: ~1,500 filas con coberturas inusuales (5-240 trimestres)

> [!WARNING]
> **Revenues con `qtrs` ≥ 5** son anomalías significativas. Un valor de `qtrs=240` significaría 60 años de ingresos acumulados, lo cual es un error en el dato XBRL de origen. Estos registros (~1,500) deben filtrarse.
> 
> **Net income y EBIT muestran el mismo patrón:** la mayoría con qtrs 1-4, pero hay registros con qtrs > 4 que son errores.

### Implicación para la anualización:
La distribución real de `qtrs` confirma que:
- Para ítems de flujo (IS/CF), `qtrs` varía entre 1 y 4 (y anomalías > 4)
- La anualización correcta es `factor = 4 / qtrs` para `qtrs = 1, 2, 3, 4`
- Los registros con `qtrs = 0` en ítems de flujo, o `qtrs > 4`, deben tratarse como datos inválidos

## 10. Outliers Extremos

| Variable | Outliers bajos | Outliers altos |
|----------|---------------|---------------|
| Assets | 6 (< $0) | 301 (> $1.5T) |
| Liabilities | 73 (< $0) | 250 (> $1.7T) |
| StockholdersEquity | 279 (< -$5.3B) | 279 (> $188.7B) |
| Revenues | 109 (< -$28.4M) | 109 (> $198.7B) |
| NetIncomeLoss | 281 (< -$3.6B) | 281 (> $17.5B) |
| CommonStockSharesOutstanding | 4 (< 0) | 216 (> 23.2B) |

> [!NOTE]
> Los outliers altos en Assets ($4.5T), Revenues ($680B) corresponden a empresas reales muy grandes (JPMorgan Chase, Walmart, Apple, etc.). **NO deben eliminarse.** Los outliers bajos (valores negativos en campos que no deberían serlo) sí deben limpiarse.

---

## Resumen de Acciones de Limpieza Necesarias

| # | Acción | Filas afectadas | Razón |
|---|--------|----------------|-------|
| 1 | Eliminar duplicados `cik+fy+fp` (quedarse con filing más reciente) | ~8,897 | Amendments reemplazan originales |
| 2 | Eliminar `Assets < 0` | 6 | Error de datos |
| 3 | Eliminar `Assets = 0` | 2,584 | Shell companies |
| 4 | Eliminar `Assets NaN` | 4,957 | Sin datos utilizables |
| 5 | NaN en `Cash < 0` | 66 | Error de datos |
| 6 | NaN en `SharesOutstanding < 0` | 4 | Error de datos |
| 7 | **Opcional:** Filtrar `fy < 2013` o `fy > 2025` | A determinar | Datos fuera de rango esperado |
| 8 | Filtrar ítems de flujo con `qtrs > 4` o `qtrs = 0` | ~1,500+ | Datos anómalos de cobertura temporal |
| 9 | Filtrar las 6 filas con `fp = Q4` | 6 | Anomalías |

**Resultado estimado:** ~290,000-295,000 filas limpias sobre ~305,000 originales.
