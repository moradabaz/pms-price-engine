# web — landing de portfolio

Página estática en Astro, sin JavaScript de cliente, que presenta el proyecto y embebe el
dashboard de Streamlit (`dashboard/`) en un iframe. No sustituye al dashboard, es una capa de
presentación delante de él.

## Desarrollo local

```bash
cd web
npm install
cp .env.example .env   # ajusta PUBLIC_DASHBOARD_URL si el dashboard no está en localhost:8501
npm run dev
```

## Build

```bash
npm run build   # genera dist/, sitio 100% estático
npm run preview
```

## Variables de entorno

- `PUBLIC_DASHBOARD_URL`: URL del dashboard de Streamlit a embeber.
- `PUBLIC_GITHUB_URL`: enlace al repo, usado en los botones de la página.

## Caveat conocido: iframe + Streamlit en producción

Streamlit no envía `X-Frame-Options` por defecto, así que el iframe funciona en local sin tocar
nada. En producción, si el dashboard y la landing viven en **dominios distintos**, el WebSocket
que Streamlit usa para las actualizaciones en vivo puede toparse con las comprobaciones de
origen de Streamlit y quedarse colgado en "Please wait...". Dos formas de evitarlo:

1. Servir ambos bajo el mismo dominio con Caddy (landing en `/`, dashboard en `/app` o en un
   subdominio con el mismo dominio raíz), o
2. Arrancar Streamlit con `--server.enableCORS=false` si siguen en dominios distintos.

No aplica en local (mismo host, puertos distintos ya funciona).

## Despliegue actual: dominios separados

`pms-price-engine.duckdns.org` sirve **Streamlit directamente** (ver `infra/aws/Caddyfile`), sin
`baseUrlPath` ni iframe — más simple y sin los problemas de WebSocket que un iframe cross-path
introducía. Esta landing de Astro (`web/`) no se despliega ahí; queda reservada para una página
personal en otro dominio, que embeberá el dashboard por iframe apuntando al dominio de DuckDNS:

```bash
PUBLIC_DASHBOARD_URL=https://pms-price-engine.duckdns.org \
PUBLIC_GITHUB_URL=https://github.com/moradabaz/pms-price-engine \
  npm run build
```

Como en ese escenario la landing y el dashboard sí viven en **dominios distintos**, aplica el
caveat de arriba: si el iframe se queda colgado en "Please wait...", arrancar Streamlit con
`--server.enableCORS=false`.
