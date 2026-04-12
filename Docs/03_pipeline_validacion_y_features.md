# Pipeline Unificado: Validación, Feature Engineering y Preparación del Dataset

**Script:** `src/3_validacion_features.py`
**Fecha:** Abril 2026
**Prerrequisito:** Ejecutar `src/2_consolidar_variables_sec.py`

---

## Objetivo

Script único que ejecuta los pasos 3, 4 y 5 del pipeline SEC DERA en una sola ejecución, sin CSVs intermedios. Todo se procesa en memoria sobre un único DataFrame.

**Entrada:** `VARIABLES_FINANCIERAS_CRUDAS.csv` (305,209 filas × 196 cols)
**Salida:** `DATASET_MODELO_LISTO.csv` (~271,375 filas × ~177 cols)

```bash
# Ejecución con valores por defecto
python src/3_validacion_features.py

# Ejecución con rutas personalizadas
python src/3_validacion_features.py --entrada ruta/VARIABLES_FINANCIERAS_CRUDAS.csv --salida ruta/DATASET_MODELO_LISTO.csv

# Ejecución con parámetros personalizados
python src/3_validacion_features.py --min-cobertura 10 --min-assets 50000 --winsor-lower 2.0 --winsor-upper 98.0
```

---

## Parámetros Configurables

| Parámetro | Default | Descripción |
|-----------|---------|-------------|
| `--entrada` | `datos_sec_edgar/VARIABLES_FINANCIERAS_CRUDAS.csv` | CSV crudo de entrada |
| `--salida` | `datos_sec_edgar/DATASET_MODELO_LISTO.csv` | CSV de salida listo para modelo |
| `--directorio` | `datos_sec_edgar` | Directorio de datos (cuando no se especifican rutas) |
| `--min-cobertura` | 15.0% | Cobertura mínima para mantener una variable `fe_*` |
| `--min-assets` | 100,000 USD | Activos mínimos para excluir shell companies |
| `--min-periodos` | 2 | Períodos mínimos por empresa |
| `--winsor-lower` | 1.0% | Percentil inferior para winsorización |
| `--winsor-upper` | 99.0% | Percentil superior para winsorización |

---

## Flujo del Pipeline

```
VARIABLES_FINANCIERAS_CRUDAS.csv (305,209 × 196)
        │
        ▼
   ┌─────────────────────────────────────┐
   │  FASE 1 — Validación y Limpieza    │
   │  9 pasos de limpieza de datos      │
   │  → 287,979 filas × 196 cols        │
   └─────────────┬───────────────────────┘
                 │  (en memoria)
                 ▼
   ┌─────────────────────────────────────┐
   │  FASE 2 — Feature Engineering      │
   │  7 bloques, ~73 variables fe_*     │
   │  → 287,979 filas × 269 cols        │
   └─────────────┬───────────────────────┘
                 │  (en memoria)
                 ▼
   ┌─────────────────────────────────────┐
   │  FASE 3 — Preparación del Dataset  │
   │  5 sub-pasos de optimización       │
   │  → ~271,375 filas × ~177 cols      │
   └─────────────┬───────────────────────┘
                 │
                 ▼
        DATASET_MODELO_LISTO.csv
```

---

## FASE 1 — Validación y Limpieza

### Paso 0 — Diagnóstico Inicial
Verificación del estado de los datos antes de cualquier modificación.

### Paso 1 — Eliminar registros con fp = Q4
**Filas eliminadas: 6.** En SEC EDGAR, el cuarto trimestre se reporta con `fp='FY'`. Los registros con `fp='Q4'` son errores de metadata XBRL.

### Paso 2 — Deduplicar cik + fy + fp
**Filas eliminadas: ~8,900.** Se conserva el filing más reciente (mayor `filed`) para resolver duplicados por amendments (`10-K/A`, `10-Q/A`).

### Paso 3 — Eliminar Assets < 0
**Filas eliminadas: 6.** Los activos totales nunca pueden ser negativos según GAAP/IFRS.

### Paso 4 — Eliminar Assets = 0
**Filas eliminadas: ~2,500.** Shell companies, SPACs sin activos reales. Provocarían divisiones por cero en ratios.

### Paso 5 — Eliminar Assets NaN
**Filas eliminadas: ~4,900.** Sin activos totales es imposible calcular la mayoría de ratios financieros.

### Paso 6 — Corregir valores imposibles (→ NaN)
**Valores modificados: ~98.** Cash negativo, acciones negativas y liabilities negativas se convierten a NaN sin eliminar la fila.

### Paso 7 — Limpiar datos de flujo con qtrs anómalos (→ NaN)
**Valores modificados: ~37,600.** Tags de flujo con `qtrs > 4` o `qtrs = 0` se anulan.

### Paso 8 — Filtrar por rango temporal
**Filas eliminadas: ~1,200.** Se conserva solo fy 2013–2025.

**Resultado Fase 1:** 305,209 → 287,979 filas (-5.6%), 12,463 empresas.

---

## FASE 2 — Feature Engineering (~73 variables)

Todas las divisiones usan `safe_div()` que retorna NaN cuando el denominador es 0 o nulo.

### Convención de Nomenclatura

| Prefijo | Categoría | Cantidad |
|---------|-----------|----------|
| `fe_ctx_` | Variables contextuales (metadata derivada) | 6 |
| `fe_anual_` | Valores anualizados (flujos × factor 4/qtrs) | 8 |
| `fe_ratio_` | Ratios financieros calculados | 27 |
| `fe_zscore_` | Componentes del Z-Score de Altman | 7 |
| `fe_flag_` | Flags binarios de alerta (0/1) | 10 |
| `fe_shares_` | Variables derivadas de acciones | 3 |
| `fe_delta_` | Cambios temporales (vs período anterior) | 12 |
| **TOTAL** | | **~73** |

### Bloque 1 — Variables Contextuales (`fe_ctx_`)

| Variable | Cobertura | Descripción |
|----------|-----------|-------------|
| `fe_ctx_revenue_consolidado` | 56.1% | Revenues unificado pre/post ASC 606 |
| `fe_ctx_fp_orden` | 100% | Orden numérico: Q1=1, Q2=2, Q3=3, FY=4 |
| `fe_ctx_periodo` | 100% | Período legible: "2021-Q1", "2021-FY" |
| `fe_ctx_filing_lag` | 100% | Días entre cierre fiscal y presentación |
| `fe_ctx_amendment` | 100% | 1 si es enmienda (10-K/A o 10-Q/A) |
| `fe_ctx_sic_sector` | 99.4% | Primeros 2 dígitos SIC (sector industrial) |

### Bloque 2 — Valores Anualizados (`fe_anual_`)

Los datos de flujo son acumulativos: se anualizan con `valor × (4 / qtrs)`.

| Variable | Cobertura | Descripción |
|----------|-----------|-------------|
| `fe_anual_revenue` | 56.1% | Ingresos proyectados a 12 meses |
| `fe_anual_ebit` | 74.9% | EBIT proyectado a 12 meses |
| `fe_anual_net_income` | 90.6% | Resultado neto proyectado a 12 meses |
| `fe_anual_cash_operating` | 82.5% | Cash Flow Operativo anualizado |
| `fe_anual_cash_investing` | 73.3% | Cash Flow de Inversión anualizado |
| `fe_anual_cash_financing` | 79.9% | Cash Flow de Financiamiento anualizado |
| `fe_anual_gross_profit` | 35.8% | Utilidad Bruta anualizada |
| `fe_anual_factor` | 95.1% | Factor de anualización aplicado |

### Bloque 3 — Ratios Financieros (`fe_ratio_`)

| Variable | Cobertura | Fórmula |
|----------|-----------|---------|
| `fe_ratio_roa` | 90.6% | NetIncome / Assets |
| `fe_ratio_roe` | 84.1% | NetIncome / Equity |
| `fe_ratio_margen_bruto` | 23.7% | GrossProfit / Revenue |
| `fe_ratio_margen_operativo` | 44.7% | EBIT / Revenue |
| `fe_ratio_margen_neto` | 49.8% | NetIncome / Revenue |
| `fe_ratio_ebitda_assets` | 100% | (EBIT + D&A) / Assets |
| `fe_ratio_liquidez` | 76.7% | AssetsCurrent / LiabilitiesCurrent |
| `fe_ratio_quick` | 77.6% | (AssetsCurrent - Inventario) / LiabilitiesCurrent |
| `fe_ratio_cash` | 84.9% | Efectivo / Assets |
| `fe_ratio_cash_current` | 68.2% | Efectivo / PasivoCirculante |
| `fe_ratio_apalancamiento` | 82.4% | Pasivos / Activos |
| `fe_ratio_deuda_equity` | 91.4% | Deuda Total / Equity |
| `fe_ratio_deuda_assets` | 100% | Deuda Total / Assets |
| `fe_ratio_deuda_cp_total` | 36.9% | Deuda CP / Deuda Total |
| `fe_ratio_cobertura_intereses` | 36.2% | EBIT / InterestExpense |
| `fe_ratio_rotacion_activos` | 56.1% | Revenue / Assets |
| `fe_ratio_sga_revenue` | 20.4% | SGA / Revenue |
| `fe_ratio_rnd_revenue` | 17.0% | R&D / Revenue |
| `fe_ratio_capex_revenue` | 35.0% | CapEx / Revenue |
| `fe_ratio_tangibilidad` | 73.1% | PPE / Assets |
| `fe_ratio_goodwill_assets` | 44.5% | Goodwill / Assets |
| `fe_ratio_intangibles_assets` | 32.1% | Intangibles / Assets |
| `fe_ratio_capital_trabajo` | 100% | Working Capital / Assets |
| `fe_ratio_fcf_assets` | 82.5% | (CFO - CapEx) / Assets |
| `fe_ratio_calidad_ingresos` | 76.4% | CFO / NetIncome |
| `fe_ratio_cashflow_deuda` | 30.9% | CFO / Deuda Total |
| `fe_ratio_cfo_revenue` | 48.2% | CFO / Revenue |

### Bloque 4 — Z-Score de Altman (`fe_zscore_`)

Z' = 0.717×X1 + 0.847×X2 + 3.107×X3 + 0.420×X4 + 0.998×X5

| Variable | Cobertura | Descripción |
|----------|-----------|-------------|
| `fe_zscore_x1_wc_assets` | 100% | Working Capital / Assets |
| `fe_zscore_x2_re_assets` | 91.7% | Retained Earnings / Assets |
| `fe_zscore_x3_ebit_assets` | 74.9% | EBIT anualizado / Assets |
| `fe_zscore_x4_equity_liab` | 75.9% | Equity / \|Liabilities\| |
| `fe_zscore_x5_rev_assets` | 56.1% | Revenue anualizado / Assets |
| `fe_zscore_altman` | 94.0% | Z-Score combinado (clip -10 a 15) |
| `fe_zscore_risk_score` | 94.0% | **⭐ TARGET: Sigmoid(Z) → 0=bajo riesgo, 1=alto riesgo** |

Se requiere ≥3 de 5 componentes con dato para calcular el Z-Score.

### Bloque 5 — Flags de Alerta (`fe_flag_`)

| Variable | Cobertura | Condición |
|----------|-----------|-----------|
| `fe_flag_patrimonio_negativo` | 91.5% | Equity < 0 |
| `fe_flag_perdida_neta` | 90.6% | NetIncome < 0 |
| `fe_flag_deficit_acumulado` | 91.7% | RetainedEarnings < 0 |
| `fe_flag_liquidez_critica` | 76.7% | Current Ratio < 1 |
| `fe_flag_fco_negativo` | 82.5% | CFO < 0 |
| `fe_flag_insolvencia` | 82.4% | Liabilities > Assets |
| `fe_flag_margen_negativo` | 74.9% | EBIT < 0 |
| `fe_flag_sin_revenue` | 100% | Revenue = 0 o NaN |
| `fe_flag_altman_distress` | 94.0% | Z-Score < 1.23 |
| `fe_flag_altman_grey` | 94.0% | 1.23 ≤ Z-Score < 2.90 |

### Bloque 6 — Variables de Acciones (`fe_shares_`)

| Variable | Cobertura | Fórmula |
|----------|-----------|---------|
| `fe_shares_book_value` | 67.6% | Equity / SharesOutstanding |
| `fe_shares_dilution` | 51.4% | (Diluted - Basic) / Basic |
| `fe_shares_assets_per_share` | 70.7% | Assets / SharesOutstanding |

### Bloque 7 — Variables Temporales (`fe_delta_`)

Cambios trimestre a trimestre por empresa, ordenados por `cik + fy + fp_orden`.

| Variable | Cobertura | Descripción |
|----------|-----------|-------------|
| `fe_delta_assets_qoq` | 95.7% | Δ% Assets vs trimestre anterior |
| `fe_delta_revenue_qoq` | 50.8% | Δ% Revenue QoQ |
| `fe_delta_net_income_qoq` | 85.7% | Δ% NetIncome QoQ |
| `fe_delta_cash_qoq` | 79.7% | Δ% Efectivo QoQ |
| `fe_delta_liabilities_qoq` | 78.1% | Δ% Pasivos QoQ |
| `fe_delta_equity_qoq` | 86.9% | Δ% Patrimonio QoQ |
| `fe_delta_liquidez_qoq` | 73.0% | Δ Current Ratio QoQ |
| `fe_delta_apalancamiento_qoq` | 78.2% | Δ Leverage QoQ |
| `fe_delta_zscore_qoq` | 89.6% | Δ Z-Score QoQ |
| `fe_delta_risk_score_qoq` | 89.6% | Δ Risk Score QoQ |
| `fe_delta_risk_deterioro` | 89.6% | 1 si riesgo empeoró |
| `fe_delta_risk_score_prev` | 89.9% | Risk Score del período anterior |

---

## FASE 3 — Preparación del Dataset

### 5.1 — Eliminación de columnas auxiliares
Se eliminan las ~92 columnas `*_qtrs` (ya usadas en Fase 2 para anualizar). Se conservan los tags XBRL crudos y las variables `fe_*`.

### 5.2 — Filtro de variables por cobertura
Con umbral default de 15%, todas las variables `fe_*` se conservan. Variables protegidas (nunca eliminadas): `fe_ctx_*`, `fe_zscore_altman`, `fe_zscore_risk_score`, `fe_flag_altman_*`.

### 5.3 — Filtros de calidad
- **Assets < $100,000:** Elimina ~16,200 filas (shell companies y micro-entidades)
- **< 2 períodos por empresa:** Elimina ~400 empresas con solo 1 filing

### 5.4 — Winsorización de outliers
Se aplica winsorización al percentil [1%, 99%] a 30 variables de tipo ratio y shares.

### 5.5 — Análisis de distribución
Reporte de distribución del `fe_zscore_risk_score`, flags de alerta y zonas Z-Score.

**Resultado final:** ~271,375 filas × ~177 cols, ~11,460 empresas.

---

## Estructura del CSV Final (~177 columnas)

```
DATASET_MODELO_LISTO.csv
├── 12 columnas de metadata (adsh, cik, name, sic, form, period, filed, fy, fp, fye, countryba, stprba)
├── ~92 tags XBRL crudos (Assets, Liabilities, NetIncomeLoss, ...)
└── ~73 variables fe_* derivadas:
    ├── 6 fe_ctx_   (contextuales)
    ├── 8 fe_anual_ (anualizadas)
    ├── 27 fe_ratio_ (ratios financieros)
    ├── 7 fe_zscore_ (Z-Score de Altman)
    ├── 10 fe_flag_  (alertas binarias)
    ├── 3 fe_shares_ (acciones)
    └── 12 fe_delta_ (temporales)
```

---

## Notas sobre la Variable Target

- `fe_zscore_risk_score` tiene media ~0.65, indicando riesgo moderado-alto en la población
- 73% de los filings caen en zona DISTRESS del Z-Score (esperado por la cantidad de empresas pequeñas/medianas)
- Para modelamiento, considerar estratificación por sector (`fe_ctx_sic_sector`)
- **Circularidad:** No usar `fe_zscore_x*` ni `fe_zscore_altman` como features si el target es `fe_zscore_risk_score`

---

## Flujo Completo del Proyecto

```
src/1_sec_edgar_downloader.py
    → Descarga ZIPs DERA (2014-2025) → datos_sec_edgar/

src/2_consolidar_variables_sec.py
    → VARIABLES_FINANCIERAS_CRUDAS.csv (305,209 × 196)

src/3_5_pipeline_completo.py  ← ESTE SCRIPT
    → DATASET_MODELO_LISTO.csv (~271,375 × ~177) 
```
