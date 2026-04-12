# 📦 Consolidador de Variables Financieras Crudas (SEC DERA)

**Script:** `consolidar_variables_sec.py`  
**Entrada:** Carpetas trimestrales en `datos_sec_edgar/` (generadas por `sec_edgar_downloader.py`)  
**Salida:** `datos_sec_edgar/VARIABLES_FINANCIERAS_CRUDAS.csv`  
**Fecha:** Abril 2026

---

## 1. Objetivo

Tomar **toda la data cruda** descargada de SEC DERA (48 trimestres, 2014Q1–2025Q4) y consolidarla en **un único CSV** con las variables financieras más relevantes para modelamiento de riesgo, **sin calcular ninguna variable derivada** (sin ratios, sin scores, sin features temporales).

Este CSV es la **materia prima** sobre la cual se construirán posteriormente:
- Ratios financieros (apalancamiento, liquidez, rentabilidad, etc.)
- Z-Score de Altman y otras métricas de riesgo
- Features temporales (crecimiento, tendencias, momentum)
- Variables objetivo para modelos ML

---

## 2. Problemas que resuelve

El consolidador original (función `consolidar_para_riesgo()`, que estaba en `1_sec_edgar_downloader.py` y fue **eliminada** por redundante) tenía 5 problemas críticos que este script corrige:

### 2.1 Filtro de `segments` (CRÍTICO)

**Problema:** El archivo `num.csv` de DERA contiene datos desglosados por segmento de negocio (región geográfica, línea de producto, etc.) en el campo `segments`. Sin filtrar, un filing puede tener 12 filas para `Assets`: una con el total consolidado (`segments=NaN`) y 11 con valores parciales por segmento.

**Impacto verificado:** En Q4-2024, el **55.9%** de las filas tienen `segments` no vacío. Esto causaba que el `aggfunc='first'` del pivot original pudiera tomar un valor de segmento parcial como si fuera el total.

**Solución:** Filtrar `segments IS NULL` antes del pivot → solo datos consolidados.

### 2.2 Dedup inteligente — reemplazo de `aggfunc='first'` (CRÍTICO)

**Problema:** El pivot original usaba `aggfunc='first'`, que toma el primer valor que pandas encuentra — dependiente del orden interno, no de lógica financiera. Verificado: el **97.4%** de combinaciones `(adsh, tag)` tenían múltiples filas.

**Solución:** Cadena de filtros explícitos:
1. Filtrar `segments=NaN` (elimina desgloses por segmento)
2. Filtrar `coreg=NaN` (elimina subsidiarias)
3. Filtrar `ddate == period` (solo datos del período actual, no comparativos)
4. Ordenar por `qtrs` descendente y quedarse con el mayor (más comprensivo)
5. `drop_duplicates` explícito por `(adsh, tag)`

Después de estos filtros, queda **exactamente un valor** por `(adsh, tag)`, haciendo el `aggfunc='first'` del pivot seguro y determinista.

### 2.3 Filtro de `coreg` (MENOR)

**Problema:** El campo `coreg` (co-registrant) indica si el dato es de una subsidiaria. Sin filtrar, se mezclan datos de la empresa principal con subsidiarias.

**Impacto verificado:** Solo 1.2% de filas afectadas en Q4-2024. Impacto bajo pero real.

**Solución:** Filtrar `coreg IS NULL`.

### 2.4 Filtro inteligente de `uom` (ALTO)

**Problema:** El filtro original `uom == 'USD'` descartaba **el 100%** de `CommonStockSharesOutstanding` (que tiene `uom='shares'`). También descartaba otros tags de conteo de acciones.

**Impacto verificado:** 29,949 filas de `CommonStockSharesOutstanding` eliminadas completamente en Q4-2024.

**Solución:** Filtro diferenciado por tipo de tag:
- Tags monetarios (balance, resultados, flujo de caja): `uom = 'USD'`
- Tags de conteo de acciones: `uom = 'shares'`

### 2.5 Preservación del campo `qtrs`

**Problema:** El consolidador original no preservaba el campo `qtrs` de `num.csv`. Este campo indica la cobertura temporal del dato:
- `qtrs=0`: dato puntual (balance — Assets, Liabilities al cierre)
- `qtrs=1`: un trimestre
- `qtrs=2`: dos trimestres (semestre)
- `qtrs=3`: nueve meses
- `qtrs=4`: año completo

Sin esta información, es imposible hacer una anualización correcta de los flujos.

**Solución:** Para cada tag, se genera una columna adicional `{tag}_qtrs` con el valor de `qtrs` usado. Esto permite al notebook posterior determinar correctamente cómo anualizar.

---

## 3. Tags XBRL incluidos (~70+)

### 3.1 Balance General (BS) — 32 tags
Datos puntuales al cierre del período (`qtrs=0`).

| Tag | Qué mide |
|-----|----------|
| `Assets` | Activos totales |
| `AssetsCurrent` | Activos corrientes (liquidables en <1 año) |
| `AssetsNoncurrent` | Activos no corrientes |
| `CashAndCashEquivalentsAtCarryingValue` | Efectivo disponible |
| `RestrictedCashAndCashEquivalentsAtCarryingValue` | Efectivo restringido |
| `ShortTermInvestments` | Inversiones a corto plazo |
| `MarketableSecuritiesCurrent` | Valores negociables corrientes |
| `AccountsReceivableNetCurrent` | Cuentas por cobrar netas |
| `InventoryNet` | Inventario neto |
| `PrepaidExpenseAndOtherAssetsCurrent` | Gastos prepagados y otros activos corrientes |
| `PropertyPlantAndEquipmentNet` | Propiedad, planta y equipo neto |
| `OperatingLeaseRightOfUseAsset` | Activo por derecho de uso (leasing operativo, ASC 842) |
| `Goodwill` | Valor llave (sobreprecio en adquisiciones) |
| `IntangibleAssetsNetExcludingGoodwill` | Intangibles netos sin goodwill |
| `OtherAssetsNoncurrent` | Otros activos no corrientes |
| `OtherAssetsCurrent` | Otros activos corrientes |
| `Liabilities` | Pasivos totales |
| `LiabilitiesCurrent` | Pasivos corrientes |
| `LiabilitiesNoncurrent` | Pasivos no corrientes |
| `AccountsPayableCurrent` | Cuentas por pagar |
| `AccruedLiabilitiesCurrent` | Pasivos acumulados corrientes |
| `ShortTermBorrowings` | Préstamos a corto plazo |
| `DebtCurrent` | Deuda corriente |
| `LongTermDebt` | Deuda a largo plazo |
| `LongTermDebtNoncurrent` | Deuda LP no corriente |
| `LongTermDebtCurrent` | Porción corriente de deuda LP |
| `OperatingLeaseLiabilityCurrent` | Obligación de leasing operativo corriente |
| `OperatingLeaseLiabilityNoncurrent` | Obligación de leasing operativo no corriente |
| `DeferredRevenueCurrent` | Ingresos diferidos corrientes |
| `DeferredRevenueNoncurrent` | Ingresos diferidos no corrientes |
| `ContractWithCustomerLiabilityCurrent` | Obligaciones contractuales con clientes (alt) |
| `OtherLiabilitiesNoncurrent` | Otros pasivos no corrientes |
| `OtherLiabilitiesCurrent` | Otros pasivos corrientes |
| `StockholdersEquity` | Patrimonio neto |
| `StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` | Patrimonio incluyendo minoritarios |
| `RetainedEarningsAccumulatedDeficit` | Utilidades retenidas / Déficit acumulado |
| `CommonStockValue` | Valor del capital social |
| `AdditionalPaidInCapital` | Prima en colocación de acciones |
| `TreasuryStockValue` | Acciones en tesorería (recompradas) |
| `MinorityInterest` | Interés minoritario |
| `AccumulatedOtherComprehensiveIncomeLossNetOfTax` | Otros resultados integrales acumulados |

### 3.2 Estado de Resultados (IS) — 27 tags
Datos acumulados durante el período (`qtrs > 0`).

| Tag | Qué mide |
|-----|----------|
| `Revenues` | Ingresos totales |
| `RevenueFromContractWithCustomerExcludingAssessedTax` | Ingresos por contratos con clientes (ASC 606) |
| `CostOfRevenue` | Costo de ventas |
| `CostOfGoodsAndServicesSold` | Costo de bienes y servicios (alternativa) |
| `GrossProfit` | Utilidad bruta |
| `OperatingExpenses` | Gastos operativos totales |
| `ResearchAndDevelopmentExpense` | Gasto en I+D |
| `SellingGeneralAndAdministrativeExpense` | Gastos de venta, generales y administrativos |
| `GeneralAndAdministrativeExpense` | Gastos generales y administrativos |
| `SellingAndMarketingExpense` | Gastos de venta y marketing |
| `DepreciationAndAmortization` | Depreciación y amortización |
| `DepreciationDepletionAndAmortization` | D&A extendida (incluye depleción) |
| `Depreciation` | Solo depreciación |
| `AmortizationOfIntangibleAssets` | Solo amortización de intangibles |
| `OperatingIncomeLoss` | Resultado operativo (EBIT proxy) |
| `InterestExpense` | Gasto en intereses |
| `InterestExpenseDebt` | Gasto en intereses de deuda |
| `InterestIncomeExpenseNet` | Ingresos/gastos por intereses netos |
| `NonoperatingIncomeExpense` | Resultado no operacional |
| `OtherNonoperatingIncomeExpense` | Otros resultados no operacionales |
| `IncomeLossFromContinuingOperationsBeforeIncomeTaxes...` | Resultado antes de impuestos |
| `IncomeTaxExpenseBenefit` | Impuesto a la renta |
| `IncomeLossFromContinuingOperations` | Resultado de operaciones continuadas |
| `NetIncomeLoss` | Resultado neto |
| `ComprehensiveIncomeNetOfTax` | Resultado integral |
| `EarningsPerShareBasic` | Ganancias por acción básicas |
| `EarningsPerShareDiluted` | Ganancias por acción diluidas |

### 3.3 Flujo de Efectivo (CF) — 21 tags
Datos acumulados durante el período (`qtrs > 0`).

| Tag | Qué mide |
|-----|----------|
| `NetCashProvidedByUsedInOperatingActivities` | Flujo de caja operativo |
| `NetCashProvidedByUsedInInvestingActivities` | Flujo de caja de inversión |
| `NetCashProvidedByUsedInFinancingActivities` | Flujo de caja de financiamiento |
| `ShareBasedCompensation` | Compensación basada en acciones (no-cash) |
| `PaymentsToAcquirePropertyPlantAndEquipment` | CAPEX |
| `ProceedsFromSaleOfPropertyPlantAndEquipment` | Ventas de activos fijos |
| `PaymentsToAcquireBusinessesNetOfCashAcquired` | Adquisiciones de negocios |
| `PaymentsOfDividends` | Dividendos pagados |
| `PaymentsOfDividendsCommonStock` | Dividendos de acciones comunes |
| `ProceedsFromIssuanceOfDebt` | Emisión de deuda |
| `ProceedsFromIssuanceOfLongTermDebt` | Emisión de deuda LP |
| `RepaymentsOfDebt` | Pagos de deuda |
| `RepaymentsOfLongTermDebt` | Pagos de deuda LP |
| `ProceedsFromIssuanceOfCommonStock` | Emisión de acciones |
| `ProceedsFromStockOptionsExercised` | Ejercicio de opciones |
| `PaymentsForRepurchaseOfCommonStock` | Recompra de acciones |
| `IncreaseDecreaseInAccountsReceivable` | Cambio en cuentas por cobrar |
| `IncreaseDecreaseInInventories` | Cambio en inventario |
| `IncreaseDecreaseInAccountsPayable` | Cambio en cuentas por pagar |
| `DeferredIncomeTaxExpenseBenefit` | Impuesto diferido |

### 3.4 Acciones (uom = 'shares') — 4 tags

| Tag | Qué mide |
|-----|----------|
| `CommonStockSharesOutstanding` | Acciones en circulación |
| `CommonStockSharesIssued` | Acciones emitidas |
| `WeightedAverageNumberOfSharesOutstandingBasic` | Promedio ponderado de acciones (básico) |
| `WeightedAverageNumberOfDilutedSharesOutstanding` | Promedio ponderado de acciones (diluido) |

---

## 4. Metadata incluida (de sub.csv)

Cada fila del CSV final incluye estos campos de identificación:

| Campo | Descripción | Ejemplo |
|-------|-------------|---------|
| `adsh` | Accession Number — ID único del filing | `0000002178-24-000096` |
| `cik` | Central Index Key — ID de la empresa en EDGAR | `2178` |
| `name` | Nombre de la empresa | `ADAMS RESOURCES & ENERGY, INC.` |
| `sic` | Código SIC de industria (4 dígitos) | `5171` |
| `form` | Tipo de formulario | `10-K`, `10-Q`, `10-K/A`, `10-Q/A` |
| `period` | Fecha fin del período (YYYYMMDD) | `20241231` |
| `filed` | Fecha de presentación (YYYYMMDD) | `20250228` |
| `fy` | Año fiscal | `2024` |
| `fp` | Período fiscal | `Q1`, `Q2`, `Q3`, `FY` |
| `fye` | Fin de año fiscal (MMDD) | `1231` |
| `countryba` | País de la empresa | `US` |
| `stprba` | Estado/provincia | `TX` |

---

## 5. Estructura del CSV de salida

```
VARIABLES_FINANCIERAS_CRUDAS.csv
├── Metadata (12 columnas)         → adsh, cik, name, sic, form, ...
├── Valores de tags (~70 columnas) → Assets, Revenues, NetIncomeLoss, ...
└── Cobertura qtrs (~70 columnas)  → Assets_qtrs, Revenues_qtrs, ...
```

Columnas `_qtrs` indican la cobertura temporal de cada valor:
- `0` = dato puntual (balance al cierre)
- `1` = un trimestre
- `2` = dos trimestres (semestre)
- `3` = nueve meses
- `4` = año completo

---

## 6. Uso

```bash
# Consolidar todo (2014Q1 a 2025Q4)
python consolidar_variables_sec.py

# Rango específico
python consolidar_variables_sec.py --inicio 2020Q1 --fin 2024Q4

# Directorio y salida personalizados
python consolidar_variables_sec.py --directorio datos_sec_edgar --salida mi_dataset.csv
```

---

## 7. Flujo de procesamiento

```
Por cada trimestre (48 total):
  ┌──────────────────────────────────────────────────────────────────┐
  │  sub.csv → Filtrar formularios (10-K/Q) → Extraer metadata      │
  │  num.csv → Filtrar tags (~70+)                                   │
  │         → Filtrar segments=NULL (solo consolidado)               │
  │         → Filtrar coreg=NULL (solo empresa principal)            │
  │         → Filtrar uom (USD para $ / shares para acciones)       │
  │         → Match ddate=period (solo período actual)              │
  │         → Dedup: max(qtrs) por (adsh, tag)                      │
  └──────────────────────────────────────────────────────────────────┘

Después de todos los trimestres:
  ┌──────────────────────────────────────────────────────────────────┐
  │  Concatenar todos los trimestres                                 │
  │  Pivot largo → ancho (una columna por tag)                      │
  │  Pivot de qtrs → columnas _qtrs                                 │
  │  JOIN con metadata de sub.csv                                    │
  │  Ordenar columnas y guardar CSV                                 │
  └──────────────────────────────────────────────────────────────────┘
```

---

## 8. Comparativa con el consolidador original (ya eliminado)

Nota: La función `consolidar_para_riesgo()` fue **eliminada** de `1_sec_edgar_downloader.py` porque era redundante — generaba un archivo `DATASET_RIESGO_CONSOLIDADO.csv` que no se utilizaba en el pipeline. Este script la reemplaza completamente.

| Aspecto | Original (eliminado) | **Actual (`2_consolidar_variables_sec.py`)** |
|---------|------|------|
| Tags XBRL | ~24 | **~70+** |
| Filtra segments | ❌ No | ✅ Sí |
| Filtra coreg | ❌ No | ✅ Sí |
| UOM inteligente | ❌ Solo USD | ✅ USD + shares |
| Dedup | `aggfunc='first'` (arbitrario) | ✅ Explícito (max qtrs) |
| Preserva qtrs | ❌ No | ✅ Columnas `_qtrs` |
| Match ddate=period | ❌ No | ✅ Solo datos actuales |
| Calcula ratios | Sí (10 ratios) | **No** (solo datos crudos) |
| Calcula Z-Score | No (notebook) | **No** |

---

## 9. Pasos recomendados a realizar

### Paso 1 — Ejecutar el consolidador ⏳
```bash
python consolidar_variables_sec.py
```
Tiempo estimado: 15-30 minutos (depende del hardware).  
Resultado: `datos_sec_edgar/VARIABLES_FINANCIERAS_CRUDAS.csv`

### Paso 2 — Exploración y validación del CSV 🔍
Crear un notebook (`02_exploracion_datos_crudos.ipynb`) que:
- Cargue el CSV y muestre dimensiones (filas × columnas)
- Verifique cobertura por variable (% de NaN por columna)
- Analice distribución de formularios (10-K vs 10-Q) y períodos (Q1-FY)
- Verifique rangos de valores (Assets > 0, no hay signos invertidos, etc.)
- Identifique outliers extremos que podrían indicar errores de datos
- Verifique que no hay filings duplicados (mismo cik + fy + fp)

### Paso 3 — Feature engineering (nuevo script/notebook) 🧮
Con el CSV crudo validado, crear un nuevo componente que calcule:
- **Ratios financieros** (~20+): apalancamiento, liquidez, rentabilidad, eficiencia, cobertura
- **EBITDA y métricas derivadas**: EBITDA, Deuda Neta/EBITDA, FCF
- **Anualización correcta**: usando las columnas `_qtrs` para proyectar flujos a base anual
- **Features temporales**: crecimiento QoQ y YoY, momentum, volatilidad
- **Features sectoriales**: ratios normalizados por mediana del sector (SIC)
- **Flags de alerta**: patrimonio negativo, pérdidas consecutivas, liquidez crítica
- **Filing lag**: `filed - period` en días (retrasos = señal de problemas)

### Paso 4 — Definir variable objetivo real 🎯
**Problema actual:** El notebook anterior usaba Z-Score como target → circularidad.
Investigar alternativas basadas en **eventos reales**:
- Filings 8-K con eventos de default/bancarrota
- Empresas que desaparecen del dataset (delisting)
- Opiniones de auditoría "going concern"
- Caídas accionarias significativas (>50% en un período)
- O el Z-Score como **una feature más**, no como target

