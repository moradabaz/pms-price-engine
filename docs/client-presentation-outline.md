# Guía de presentación PowerPoint — Profitable Dynamic Pricing Engine (PoC)

Audiencia: cliente BiLemon (negocio + técnico). Objetivo: demostrar que el PoC valida el
principio central del spec y dar visibilidad de qué falta para producción.

## Estructura sugerida (12-14 slides)

**1. Portada**
Profitable Dynamic Pricing Engine — PoC técnico. Fecha, versión spec referenciada (v1.0 Ago 2026).

**2. Objetivo del PoC**
Validar que la rentabilidad puede integrarse en la construcción del precio (no verificarse
después) usando datos en streaming reales. Recordar el principio central del cliente:
"el mercado determina cuánto podemos vender; la estructura económica determina por debajo de
qué precio no deberíamos vender."

**3. Arquitectura end-to-end (1 diagrama)**
Postgres → Debezium/CDC → Kafka → Flink (streaming) → DynamoDB (hot) → Iceberg/S3 (cold/audit)
→ dbt → Dashboard. Un único diagrama, sin texto denso — usar el que ya generamos
(`diagrams/pms-price-engine-architecture.html`) como base visual.

**4. El core económico: BER / MPR / MPA**
Mostrar la fórmula tal cual el spec del cliente (§11) y confirmar que es exactamente la
implementada (ADR-0013). Usar el ejemplo LOS 1 vs LOS 5 del propio documento del cliente
(cleaning+laundry 110€, floor 188€ vs 59€) — mismo lenguaje que ellos ya conocen.

**5. LOS-aware Floor Matrix (demo)**
Screenshot del dashboard mostrando floor distinto por duración de estancia. Mensaje: "el mismo
día puede ser inviable a 1 noche y rentable a 5."

**6. Channel Pricing / gross-up**
Ejemplo Direct vs Booking.com con distinto floor por fees de canal — mismo ejemplo numérico
que el spec (§16).

**7. Revenue Management por capas**
Mostrar las capas implementadas (Structural, Market, Commercial, Guardrails) vs. las capas
del spec (A-G). Ser honestos: Performance e Inventory existen como estructura pero sin señal
real todavía; falta la capa Booking Window.

**8. Explainability**
Screenshot de una decisión con sus `DecisionComponent` / reason codes. Mensaje: "cada precio es
reconstruible y auditable" — requisito explícito del cliente (§19, §24).

**9. Auditoría y trazabilidad**
Iceberg como registro completo (raw, versionado, consultable con dbt/Athena). Cubre el
requisito de auditabilidad (§24: "reproducible con la misma versión de datos/reglas").

**10. Cobertura vs. spec del cliente (resumen)**
Tabla condensada de 3-4 filas con % o semáforo por bloque: Economía del coste (✅),
Revenue Management (🟡), Estrategia/configuración (🟡), Market data real (🟡).
Usar `docs/client-spec-gap-analysis.md` como fuente, pero simplificar a nivel ejecutivo —
no llevar la tabla de 15 filas a la reunión, resumir en 4-5 bloques.

**11. Qué falta para producción (roadmap)**
Los 8 gaps priorizados del análisis: entidades de configuración (Strategy/Rule/StayCandidate),
capa Booking Window, alertas mercado-vs-floor, comp-set real, versionado, API de simulación,
más bases contractuales, señal real en Performance/Inventory.

**12. Estado del despliegue**
Todo corre local (Docker + LocalStack) por control de costes durante el PoC; existe un plan
Terraform ya diseñado (ADR-0010) para un despliegue real en AWS bajo demanda, aún no ejecutado.

**13. Próximos pasos propuestos**
Priorización sugerida de los gaps de la slide 11 en función de qué valida antes el negocio
(sugerencia: Booking Window + Strategy panel primero, por ser los de mayor impacto visible).

**14. Preguntas / cierre**

## Recomendaciones de formato

- Máximo 1 idea por slide, apoyada en 1 dato o 1 captura — evitar bloques de texto largo.
- Reutilizar literalmente los términos del documento del cliente (BER, MPR, Profitable Floor,
  Stay Candidate) para que reconozcan su propio vocabulario.
- Las slides 10-11 (gaps) van con tono constructivo: "arquitectura ya soporta esto, falta
  construir el componente", no "no está hecho".
- Llevar el dashboard vivo como demo de apoyo si es posible, en vez de solo capturas.
