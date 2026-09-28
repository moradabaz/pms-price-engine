# Gap Analysis: Implementación vs. Spec del Cliente (BiLemon "Profitable Dynamic Pricing Engine" v1.0, Ago 2026)

Fuente cliente: `dynamic_price_engine.md` (BiLemon Hospitality Services & Technology).
Base de comparación: estado del repo a Phase 20 / ADR-0013 (2026-09-14).

## Veredicto resumido

El **núcleo económico del motor (BER/MPR/MPA, floor por LOS, channel gross-up, explicabilidad)
coincide casi exactamente con la especificación del cliente**, incluyendo las mismas fórmulas
(§11) y el mismo ejemplo LOS 1 vs LOS 5. La **capa de Revenue Management (capas A-G)** y la
**arquitectura de costes** están construidas y parcialmente alineadas. Lo que falta es sobre
todo **modelado explícito de entidades de negocio** (Strategy, Rule, StayCandidate como objetos
de primera clase), la **capa de Booking Window** (lead time / last minute), la **resolución de
conflictos mercado-vs-floor** (§13) y una **API de simulación on-demand**. En volumen de
funcionalidad, el PoC cubre aproximadamente el **65-70% del MVP** descrito en §25.1.

## Tabla de cumplimiento por área

| # | Área (spec) | Estado | Evidencia | Ruta en el código |
|---|---|---|---|---|
| 1 | Property Pricing Profile / Bonus-Malus / PRP (§6-7) | ✅ Implementado | `property_attribute_factor()` × `avg_nightly_rate_eur` → `property_reference_price()` (Phase 8) | `libs/pricing-formulas/src/pricing_formulas/layers/structural.py` |
| 2 | Arquitectura de costes multi-dimensional (§8) | 🟡 Parcial | `CostDefinitionRow`/`CostAllocationRuleRow`/`CompanyCostOccurrenceRow` (Phase 17/19) cubren scope/behavior/trigger/calculation_base/recurrence; falta reparto explícito de Company Costs en el floor | `streaming/flink-jobs/src/flink_jobs/models.py`; enriquecimiento en `streaming/flink-jobs/src/flink_jobs/stage_cost_enrichment.py` |
| 3 | Stay Candidate como entidad (§9) | ❌ No implementado | No existe clase `StayCandidate`; agregación de costes vive en el estado de Flink (`CostAggregate`), no como objeto Property+Arrival+LOS+Channel+Guests | `CostAggregate` en `streaming/flink-jobs/src/flink_jobs/models.py` (lo más cercano) |
| 4 | LOS-aware floor matrix / dilución de costes fijos (§10) | ✅ Implementado | `LOS_CANDIDATES`, `LosFloorCandidate`, `recommend_minimum_stay()` (Phase 9/15) | `libs/pricing-formulas/src/pricing_formulas/engine.py`; consumido en `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py` |
| 5 | BER/MPR/MPA + bases contractuales del owner (§11) | 🟡 Parcial (fórmula exacta, bases limitadas) | `BER=Costes/(1-p)`, `MPR=Costes/(1-p-m)` — coincide con ADR-0013; `RevenueBase` solo soporta 3 bases de las ~5 del spec | Fórmulas: `libs/pricing-formulas/src/pricing_formulas/engine.py`; bases contractuales: `libs/pricing-formulas/src/pricing_formulas/layers/commercial.py`; enriquecimiento: `streaming/flink-jobs/src/flink_jobs/stage_owner_contract_enrichment.py` |
| 6 | Tabla de acción mercado-por-debajo-de-floor (§13) | ❌ No implementado | No hay clasificación/alerta de conflicto RM<Floor más allá del recomendador de min-stay | — (no existe módulo; el candidato más cercano es `recommend_minimum_stay()` en `engine.py`) |
| 7 | Revenue Management capas A-G (§14) | 🟡 Parcial | Existen structural/market/performance/inventory/commercial/guardrails; **falta capa Booking Window (D)**; performance/inventory son stubs (`×1.0`); no hay orquestación priority/compatibility/caps entre reglas | `libs/pricing-formulas/src/pricing_formulas/layers/{structural,market,performance,inventory,commercial,guardrails}.py` |
| 8 | Panel de Pricing Strategy (§15) | ❌ No implementado como entidad | `target_margin` es parámetro de función, no existe `PricingStrategy` configurable (Objective, Positioning, Caps, Override permissions) | Parámetro suelto en `libs/pricing-formulas/src/pricing_formulas/engine.py` (firma de `decide_price*`); sin entidad ni UI dedicada |
| 9 | Channel Pricing / gross-up (§16) | ✅ Implementado | Phase 16: `channel_rates_eur`, netting de comisión por canal | `streaming/flink-jobs/src/flink_jobs/models.py` (campo en `NightSnapshot`/similar), cálculo en `libs/pricing-formulas/src/pricing_formulas/engine.py` |
| 10 | Market Data / Comp-set (§17) | 🟡 Parcial, sintético | `MarketSnapshot` existe pero datos sintéticos, sin filtrado like-for-like ni percentiles | Modelo: `streaming/flink-jobs/src/flink_jobs/models.py`; generador sintético: `services/market-ingestor/` |
| 11 | Final Price Decision determinista + auditable (§18) | ✅ Implementado | `PriceDecision` persistido en DynamoDB + Iceberg, funciones puras deterministas | Schema: `libs/shared-schemas/src/shared_schemas/price_decision.py`; construcción: `streaming/flink-jobs/src/flink_jobs/stage_price_decision.py`; sink cold path: `services/lakehouse-consumer/src/lakehouse_consumer/{transform,schema}.py` |
| 12 | Explainability / reason codes (§19) | ✅ Implementado | Enum `ReasonCode` + `DecisionComponent(code,label,impact)` | `libs/pricing-formulas/src/pricing_formulas/decision_components.py`; expuesto también en `libs/shared-schemas/src/shared_schemas/price_decision.py` |
| 13 | Triggers de recálculo (§20) | 🟡 Parcial | Nueva reserva/coste/contrato/canal disparan recálculo (event-driven); faltan trigger de "nuevo market snapshot", "evento" y "paso del tiempo" (lead time) | Orquestación de stages: `streaming/flink-jobs/src/flink_jobs/job.py` |
| 14 | Entidades funcionales (§21) | 🟡 8/12 presentes | Presentes: CostDefinition, CostAllocationRule, OwnerContractRule, ManualOverride, MarketSnapshot, PriceDecision, DecisionComponent, (parcial) PropertyPricingProfile vía `SegmentAssignment`. **Faltan**: CostOccurrence genérico, ChannelCostRule distinto, PricingStrategy, PricingRule, StayCandidate | La mayoría en `streaming/flink-jobs/src/flink_jobs/models.py`; `ManualOverride` también en `stage_manual_override_enrichment.py`; `SegmentAssignment` además usado en `dashboard/src/dashboard/{app,hot_path}.py` |
| 15 | Requisitos no funcionales (§28) | 🟡 Parcial | Auditoría en Iceberg ✅, rounding monetario ✅; **sin** versionado de reglas/estrategia, **sin** API de simulación on-demand (solo streaming/batch) | Auditoría: `services/lakehouse-consumer/` + `services/lakehouse-maintenance/`; no hay servicio `api`/`simulation` en `services/` |
| §25.1 MVP | 9/11 ítems | Faltan: panel de Strategy dedicado, integración real de publicación al PMS (hoy solo dashboard) | Publicación actual: `dashboard/src/dashboard/app.py` (solo lectura/visualización, no push a PMS) |

## Top gaps frente al cliente

1. Modelar **PropertyPricingProfile, PricingStrategy, PricingRule, StayCandidate** como entidades explícitas (hoy la lógica está repartida en funciones/stages de Flink).
2. **Capa Booking Window (lead time / last minute)** — inexistente.
3. **Resolución de conflicto mercado-vs-floor** (§13) — sin alertas de viabilidad.
4. **Market data real** (scraping/proveedor con comp-set filtrado) en vez de sintético.
5. **Versionado** de estrategia/reglas — no encontrado en el código.
6. **API de simulación what-if** on-demand — el sistema es puramente streaming/batch.
7. Ampliar **bases contractuales del owner** (solo 3 de ~5 del spec).
8. Las capas **Performance e Inventory** son stubs sin señal real (occupancy/pickup/pace, gaps).

## Conclusión para el cliente

El PoC demuestra que **el principio central del spec — rentabilidad integrada en la construcción
del precio, no verificada después — ya funciona end-to-end** con datos reales de streaming
(CDC + Flink + Iceberg). Los gaps restantes son mayormente de **cobertura funcional** (más capas
RM, más entidades de configuración) y no de **arquitectura**: el pipeline elegido soporta
añadir estas piezas sin rediseño.
