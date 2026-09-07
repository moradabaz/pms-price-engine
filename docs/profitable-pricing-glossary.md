# Glosario — Profitable Dynamic Pricing (explicado sin jerga del sector)

**Para quién es este documento:** alguien sin experiencia previa en alquiler vacacional / revenue
management que necesita entender qué significan los conceptos de
`docs/adr/ADR-0011-profitable-pricing-target-architecture.md` y `docs/post-poc-roadmap.md`, y por
qué importan.

**Qué no es:** ni un ADR (no registra una decisión tomada) ni un spec de fase (no define criterios
de aceptación para implementar). Es un mapa de conceptos, en español llano, con su estado actual en
este repo. Cuando un concepto ya está implementado, se referencia la fase; cuando no, se referencia
el ítem del backlog en `post-poc-roadmap.md`.

---

## 1. La idea central en una frase

En alquiler vacacional, dos motores compiten por decidir el precio de una noche: **el mercado**
(cuánto está dispuesta a pagar la gente) y **el coste real de esa reserva concreta** (limpieza,
comisión de la OTA, parte proporcional del alquiler del piso, etc.). Un motor de pricing
"normal" solo mira el mercado. Este proyecto construye uno que **nunca deja que el precio de
mercado tumbe el precio por debajo de lo que cuesta operar esa reserva** — y que además explica por
qué llegó a ese número.

---

## 2. La escalera de precios (de abstracto a concreto)

Son 6 conceptos que **no son sinónimos**, aunque a veces se confundan. Cada uno es un paso más
concreto que el anterior:

| Concepto | En una frase | Analogía |
|---|---|---|
| **Market Reference Price** | Lo que cobran pisos parecidos en la misma zona ahora mismo. | El precio "de mercado" de un coche de segunda mano, según el modelo. |
| **Property Reference Price** | El anterior, ajustado porque *este* piso concreto es mejor o peor que la media (tiene piscina, no tiene ascensor, etc.). | El mismo coche, pero corregido por su estado real y sus extras. |
| **Base Price** | El Property Reference Price, ajustado por la estrategia comercial elegida (agresiva, conservadora...). | El precio de salida que pondrías en el anuncio antes de negociar. |
| **Break-Even ADR** | El precio mínimo para **no perder dinero** en esa reserva concreta. | El precio al que un vendedor de coches ni gana ni pierde. |
| **Profitable Floor** | El Break-Even más el margen mínimo que el negocio exige ganar. | El precio mínimo al que el vendedor acepta vender y sigue durmiendo tranquilo. |
| **Final Rate** | El precio que de verdad se publica, después de aplicar mercado, canal y reglas — nunca por debajo del Floor (salvo excepción autorizada). | El precio final en la etiqueta. |

**Por qué importa:** herramientas como PriceLabs o Beyond solo calculan hasta "Base Price" ajustado
por mercado — no conocen el coste real de *esa* reserva, así que a veces recomiendan precios por
debajo del Break-Even sin saberlo. Este proyecto calcula la escalera completa.

---

## 3. Bonus/Malus (Property Attribute Factor)

**Qué es:** un multiplicador que sube o baja el precio de mercado según las características propias
del piso — terraza, piscina, planta, ruido, calidad percibida (reviews) — en vez de tratar todos los
pisos de una zona como si valieran lo mismo.

**Ejemplo:** si el mercado paga 185 €/noche de media en la zona, pero este piso concreto no tiene
ascensor ni parking, el Bonus/Malus podría bajarlo a ~157 €. Ese es el "valor estructural" del piso,
no el precio final de una noche concreta.

**Estado:** implementado — [Fase 8](../specs/phases/08-property-bonus-malus/spec.md).

---

## 4. Coste de una estancia (Stay Cost) y por qué la duración importa (LOS)

**LOS = Length of Stay**, es decir, cuántas noches dura la reserva.

**La idea contraintuitiva:** un coste fijo de la reserva (limpieza + lavandería, por ejemplo 110 €)
pesa muchísimo si solo se queda 1 noche (110 €/noche) pero casi nada si se queda 10 noches
(11 €/noche). Por eso **la misma fecha puede ser rentable para una reserva larga e inviable para una
de una sola noche**, con el mismo precio por noche.

**Consecuencia práctica:** el "precio mínimo rentable" no es un número único por piso — es una
**matriz**: uno por cada combinación de fecha de llegada y número de noches.

**Estado:** implementado — [Fase 9](../specs/phases/09-los-floor-matrix/spec.md) calcula esa matriz.
Lo que **no** está hecho todavía: usar esa matriz para *recomendar activamente* subir la estancia
mínima cuando una noche suelta no es rentable (ver `post-poc-roadmap.md`, ítem nuevo "min-stay
lever").

---

## 5. Break-Even y Profitable Floor (la fórmula, en palabras)

- **Break-Even Revenue:** cuánto hay que facturar como mínimo para cubrir gastos. Si además hay
  costes que son un % de lo que se cobra (comisión de la OTA, comisión de cobro), la fórmula no es
  "sumar costes" — hay que **dividir** entre lo que queda después de esos porcentajes, porque cuanto
  más subes el precio, más sube también la comisión.
- **Profitable Floor:** lo mismo, pero exigiendo además el margen de beneficio mínimo del negocio,
  no solo cubrir gastos.

**Estado:** implementado, con la fórmula correcta (división, no multiplicación) — fijado en
ADR-0009 tras detectar y corregir un error de cálculo previo.

---

## 6. Owner Contract (contrato con el propietario del piso)

**El problema:** la mayoría de gestoras (property managers) no se quedan con toda la reserva —
pagan una comisión o un "payout" al dueño del piso. Pero esa comisión **no siempre se calcula sobre
lo mismo**: puede ser sobre el total de la reserva, sobre el total menos la comisión de la OTA,
sobre el total menos OTA y limpieza, etc. Si el motor de pricing asume la base equivocada, calcula
mal cuánto le queda realmente a la gestora — y por tanto calcula mal el floor.

**Estado:** implementado para los casos habituales (bases cerradas y conocidas) —
[Fase 11](../specs/phases/11-owner-contract/spec.md). Deliberadamente **no** implementado: un
"solver" genérico para fórmulas de contrato no estándar/no lineales — decisión consciente, no
olvido (documentado en el propio spec de Fase 11).

---

## 7. Revenue Management por capas

**Qué es "Revenue Management":** el conjunto de reglas que ajustan el precio hacia arriba o hacia
abajo según el contexto — temporada alta, fin de semana, poca ocupación propia, reserva de última
hora, un evento cercano (concierto, feria), etc. Es la parte "inteligente" que reacciona al momento,
por encima del precio estructural del piso.

**Por qué "por capas":** si cada regla suma o resta un porcentaje sin control, se pueden acumular
descuentos hasta vender por debajo de coste sin que nadie lo note. Organizarlo en capas (estructura →
mercado → rendimiento propio → antelación → disponibilidad → promociones → guardarraíles) permite
poner límites por capa y un límite global, y sobre todo **impedir que el resultado final cruce el
Profitable Floor** salvo excepción autorizada.

**Estado:** implementado — [Fase 13](../specs/phases/13-layered-rm-engine/spec.md).

---

## 8. Pricing por canal y "gross-up"

**El problema:** vender por Booking.com no es lo mismo que vender directo. Booking se lleva una
comisión mucho mayor que cobrar directamente con tarjeta (en el ejemplo del spec: ~17% vs ~2%). Si
se aplica el mismo precio en los dos canales, en uno se gana mucho menos margen sin que se note en
el precio publicado.

**"Gross-up"** es simplemente: subir el precio publicado en el canal caro lo justo para que, después
de que el canal se lleve su comisión, quede el mismo margen que en el canal barato. No es un markup
arbitrario — es matemática derivada del coste real de vender por ese canal.

**Estado:** no implementado — el sistema hoy usa una comisión única, ciega al canal. Ítems #2 y #8
del backlog (`post-poc-roadmap.md`).

---

## 9. Guardarraíles (Guardrails) y política de Floor Hard/Soft

**Qué es:** la regla que decide qué pasa cuando el precio que "quiere" el mercado o las reglas
comerciales cae por debajo del Profitable Floor.

- **Hard floor:** nunca se publica por debajo del floor, punto.
- **Soft floor:** se puede vender por debajo, pero solo con autorización explícita y dejando
  constancia de la pérdida esperada — nunca en silencio.

**Estado:** implementado — [Fase 12](../specs/phases/12-floor-policy/spec.md).

---

## 10. Explicabilidad (Decision Components / reason codes)

**El problema que resuelve:** un gestor de propiedades no va a confiar en un precio que "la máquina
dijo que sí" sin más. Necesita poder ver: "este precio subió un 10% por temporada, bajó un 3% por
baja ocupación, y el suelo de rentabilidad era 142 € para esta combinación de fecha/canal/duración".

**Qué son los "reason codes":** en vez de solo un texto libre, cada ajuste queda guardado como un
dato estructurado (p. ej. `RULE_SEASON_HIGH`, `FLOOR_PROTECTED`) que se puede consultar, filtrar y
auditar — no solo leer.

**Estado:** implementado — [Fase 10](../specs/phases/10-decision-components/spec.md).

---

## 11. Manual Override (excepción manual autorizada)

**Qué es:** a veces el negocio quiere vender deliberadamente por debajo del floor de rentabilidad
— por ejemplo, para no dejar un piso vacío antes de un evento, o por una relación comercial
puntual. Eso debe poder hacerse, pero **con un usuario responsable, un motivo, una fecha de
caducidad, y quedando registrada la pérdida esperada** — nunca como una excepción silenciosa que
rompe la confianza en el sistema.

**Estado:** no implementado. Hoy el sistema es de solo lectura aguas abajo del motor de precios —
no existe ningún camino para que un humano escriba una excepción de vuelta al sistema. Es el cambio
de arquitectura más grande pendiente (ítem #9 del backlog).

---

## 12. Market Snapshot / comp-set

**Comp-set** ("competitive set"): el grupo de pisos comparables (misma zona, tamaño, calidad) que se
usa como referencia de mercado — comparar con la media de *toda* la ciudad sin filtrar sería
comparar peras con manzanas.

**Market Snapshot:** una "foto" del mercado en un momento dado (precio medio, percentiles,
ocupación, fuente y fecha de la foto), para poder auditar más tarde con qué información se decidió
un precio.

**Estado:** el modelo de datos ya está preparado (percentiles, fuente, fecha de captura) —
[Fase 3](../specs/phases/03-market-ingestion/spec.md) — pero la fuente sigue siendo sintética
(segmentos simulados), no datos reales de mercado. Ítem #11 del backlog.

---

## 13. Glosario rápido de siglas

| Sigla | Significado |
|---|---|
| **ADR** | Average Daily Rate — precio medio por noche. |
| **LOS** | Length of Stay — número de noches de la reserva. |
| **OTA** | Online Travel Agency — Booking, Airbnb, Vrbo, etc. |
| **RM** | Revenue Management. |
| **PMS** | Property Management System — el software de gestión del piso/reservas. |
| **PoC** | Proof of Concept — este proyecto, una prueba técnica, no el producto final. |

---

## Cómo se relaciona con el resto de la documentación

- `docs/adr/ADR-0011-profitable-pricing-target-architecture.md` — decisión de adoptar el spec
  externo como arquitectura objetivo.
- `docs/post-poc-roadmap.md` — backlog priorizado, ítem por ítem, con qué está hecho y qué falta.
- Este documento — el "traductor" de esos dos para alguien nuevo en el dominio.
