# docs/printable — material de estudio

Copias (no movidas — los originales siguen en su sitio y siguen siendo la fuente de verdad)
de los `.md` del repo que más aportan para dos cosas: afianzar conceptos de streaming, y entender
cómo se ha hecho spec-driven development a lo largo de este proyecto. Pensado para leer offline/
imprimir, no para navegar el repo día a día.

## `streaming/` — conceptos de procesamiento en streaming

Orden de lectura sugerido:

1. **`windowing-design-checklist.md`** — la guía general de cómo tomar decisiones de ventana
   (Dataflow Model, acumuladores, watermarks/idleness, dedup). Punto de partida.
2. **`keyed-state-and-checkpointing.md`** — estado con clave, RocksDB, checkpointing exactly-once.
3. **`phase-4-streaming-design-decisions.md`** + **`04-flink-processing-spec.md`** — el primer job
   de Flink del proyecto (pricing), sin ventanas ni Async I/O — la base antes de complicarlo.
4. **`26-market-pulse-job-spec.md`** + **`26-market-pulse-job-TODO.md`** — el job donde se
   introducen ventanas, watermarks/idleness y Async I/O (lo que has ido construyendo tú mismo,
   paso a paso, con las 4 decisiones de diseño documentadas en detalle).
5. **`interview-prep-streaming-lessons.md`** — resumen destilado de lecciones de streaming, en
   formato preguntas/respuestas.
6. **ADRs** (`ADR-0001-kafka-kinesis-split.md`, `ADR-0002-pyflink-over-java-flink.md`,
   `ADR-0008-kinesis-kafka-bridge.md`) — por qué Kafka+Kinesis en vez de solo uno, por qué PyFlink
   y no Java, por qué existe un bridge en vez de un conector nativo.
7. **`anticipated-risks-flink-processing.md`** + **`flink-operational-checklist.md`** — riesgos
   previstos y checklist operativo antes de dar un job por bueno.
8. El resto son **incidentes reales documentados** (Debezium, DynamoDB Streams, LocalStack,
   Kafka retention, build de la imagen PyFlink) — la parte más valiosa para aprender qué falla de
   verdad en streaming y por qué, no solo la teoría feliz.

## `spec-driven-development/` — cómo se ha especificado y documentado cada fase

- **`README.md`** — el punto de entrada del proyecto, cómo está organizado.
- **`specs/`** — las 26 specs de fase, en orden (`01-mock-app-db` → `26-market-pulse-job`). Léelas
  en orden y verás cómo el formato de spec se va afinando fase a fase (criterios de aceptación,
  decisiones de diseño explícitas, diagramas) — es el mejor ejemplo de spec-driven development que
  tienes a mano, porque es tuyo.
- **`design-decisions/`** — los documentos complementarios `phase-N-*-design-decisions.md` que
  explican el "por qué" detrás de decisiones concretas de varias fases, cuando la spec sola se
  quedaba corta.
- **`adr/`** — las 18 Architecture Decision Records del proyecto. A diferencia de una spec de fase
  (qué se construye y cómo se verifica), un ADR es una decisión arquitectónica puntual con
  alternativas rechazadas — un formato distinto y complementario que vale la pena distinguir.
- **`client-spec-gap-analysis.md`** — cómo se audita una spec ya escrita contra lo que un cliente
  real necesitaría, útil para ver el "spec-driven development" desde el lado de la revisión, no
  solo de la escritura.
- **`post-poc-roadmap.md`** — cómo se plantea la transición de una PoC spec-driven a producto real.
- **`AUDIT_DIARY.md`** — el registro histórico de verificaciones en vivo, fase a fase. Es el
  contrapunto necesario a la spec: una spec dice qué se iba a construir, esto dice qué se comprobó
  de verdad y cuándo.
