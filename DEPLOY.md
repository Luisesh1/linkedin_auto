# Deploy productivo en VPS Ubuntu

Guía para desplegar `autoLinkedin` en un VPS Ubuntu usando Docker Compose. La app conserva el puerto `5000`, pero el servicio queda publicado solo en `127.0.0.1:5000`; Caddy externo se encarga del dominio, TLS y reverse proxy.

## Arquitectura

- `docker compose` construye y ejecuta la app como `autolinkedin-app`.
- Gunicorn sirve Flask dentro del contenedor.
- Xvfb + Playwright permiten automatizar LinkedIn con Chromium en modo headless.
- Docker publica la app únicamente en `127.0.0.1:${APP_HOST_PORT:-5000}:5000`.
- Caddy, fuera de este compose, debe apuntar a `http://127.0.0.1:5000`.
- Los datos persistentes viven en `data/`, `static/generated/` y `static/debug/`.

## Requisitos del VPS

- Ubuntu actualizado.
- Git.
- Docker Engine.
- Docker Compose plugin (`docker compose`, no `docker-compose` legacy).
- Caddy externo instalado en el host o en otra composición Docker.
- Dominio apuntando al VPS si se va a publicar por HTTPS.

Instalación base recomendada:

```bash
sudo apt update
sudo apt install -y git ca-certificates curl
```

Instala Docker siguiendo la documentación oficial de Docker para Ubuntu. Al terminar, valida:

```bash
docker --version
docker compose version
```

## Preparar el proyecto

Clona el repositorio en el directorio donde vivirá la app:

```bash
git clone git@github.com:Luisesh1/linkedin_auto.git autolinkedin
cd autolinkedin
```

Crea el archivo de entorno y los directorios persistentes:

```bash
cp .env.example .env
mkdir -p data static/generated static/debug
```

Revisa la configuración final de Docker Compose:

```bash
docker compose config
```

## Configurar `.env`

Edita `.env` y reemplaza los valores de ejemplo. Como mínimo configura:

```env
LINKEDIN_EMAIL=tu@email.com
LINKEDIN_PASSWORD=tu_contraseña_de_linkedin
XAI_API_KEY=tu_api_key_xai
APP_SECRET_KEY=una_clave_larga_aleatoria
APP_TIMEZONE=America/Mexico_City
ADMIN_USERNAME=luisesh1
ADMIN_PASSWORD_HASH=pega_aqui_un_hash_werkzeug
```

Para producción conserva estos valores:

```env
APP_HOST_PORT=5000
APP_DEBUG=false
APP_HEADLESS=true
APP_CONFIG_PATH=/app/data/config.yaml
DB_PATH=/app/data/posts.db
LINKEDIN_SESSION_DIR=/app/data/linkedin_session
LINKEDIN_HISTORY_FILE=/app/data/post_history.json
SECURITY_REQUIRE_HTTPS_COOKIES=true
GUNICORN_THREADS=4
GUNICORN_TIMEOUT=180
```

Genera el hash de la contraseña admin desde el host:

```bash
docker run --rm python:3.12-slim sh -c "pip install werkzeug >/dev/null && python -c \"from werkzeug.security import generate_password_hash; print(generate_password_hash('CAMBIA_ESTA_PASSWORD'))\""
```

Copia el resultado completo en `ADMIN_PASSWORD_HASH`.

## Levantar producción

Construye la imagen y arranca el contenedor:

```bash
docker compose up -d --build
```

Consulta el estado:

```bash
docker compose ps
docker compose logs -f app
```

Valida el healthcheck local:

```bash
curl http://127.0.0.1:5000/healthz
```

La respuesta esperada es:

```json
{"ok":true}
```

## Configurar Caddy externo

Ejemplo mínimo de `Caddyfile` en el host:

```caddyfile
tudominio.com {
	reverse_proxy 127.0.0.1:5000
}
```

Recarga Caddy después de editar:

```bash
sudo caddy reload --config /etc/caddy/Caddyfile
```

Si Caddy corre en otro contenedor, asegúrate de que pueda llegar al servicio publicado en el host. En ese caso puedes usar una red Docker compartida o el gateway del host según tu layout.

## Datos persistentes y backups

Estos paths se deben conservar entre despliegues:

```text
data/config.yaml
data/posts.db
data/post_history.json
data/linkedin_session/
static/generated/
static/debug/
```

Backup básico:

```bash
mkdir -p backups
tar -czf backups/autolinkedin-$(date +%Y%m%d-%H%M%S).tar.gz data static/generated static/debug .env
```

No subas `.env`, `data/`, `static/generated/` ni `static/debug/` al repositorio.

## Operación diaria

Ver logs:

```bash
docker compose logs -f app
```

Reiniciar la app:

```bash
docker compose restart app
```

Detener la app:

```bash
docker compose down
```

Actualizar desde Git:

```bash
git pull
docker compose up -d --build
docker compose ps
curl http://127.0.0.1:5000/healthz
```

Ver configuración efectiva:

```bash
docker compose config
```

## Seguridad mínima

- Mantén público solo Caddy en los puertos `80` y `443`.
- No publiques el puerto `5000` en `0.0.0.0`; debe quedar como `127.0.0.1:5000`.
- Usa `SECURITY_REQUIRE_HTTPS_COOKIES=true` cuando el acceso real sea por HTTPS mediante Caddy.
- Usa una contraseña admin fuerte y guarda solo `ADMIN_PASSWORD_HASH`.
- No guardes secretos reales en `config.yaml.example`, `.env.example` ni commits.
- Haz backups antes de actualizar el VPS o reconstruir el servicio.

Ejemplo de firewall con UFW:

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
sudo ufw status
```

## Troubleshooting

El contenedor aparece `unhealthy`:

```bash
docker compose ps
docker compose logs --tail=200 app
curl -v http://127.0.0.1:5000/healthz
```

El puerto `5000` ya está ocupado:

```bash
sudo ss -ltnp | grep ':5000'
```

Si necesitas usar otro puerto local para Caddy, cambia `APP_HOST_PORT` en `.env` y actualiza el `reverse_proxy` de Caddy al mismo puerto.

Problemas de permisos en `data/`:

```bash
sudo chown -R "$(id -u):$(id -g)" data static/generated static/debug
docker compose restart app
```

Faltan variables o la app no permite login:

```bash
docker compose logs --tail=200 app
grep -E 'ADMIN_USERNAME|ADMIN_PASSWORD_HASH|APP_SECRET_KEY|XAI_API_KEY|LINKEDIN_EMAIL' .env
```

Error de sesión de LinkedIn:

```bash
docker compose logs -f app
```

Entra al panel, inicia sesión de nuevo y deja que se regenere `data/linkedin_session/`. Evita borrar esa carpeta salvo que quieras forzar una sesión nueva.

Error de Caddy o dominio:

```bash
curl http://127.0.0.1:5000/healthz
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl status caddy
sudo journalctl -u caddy -n 100 --no-pager
```

Si `curl http://127.0.0.1:5000/healthz` funciona pero el dominio no, el problema está en Caddy, DNS o firewall, no en el contenedor de la app.
