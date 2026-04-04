# ── Stage 1: build Vue ────────────────────────────────────────────────────────
FROM node:22-slim AS ui-build
WORKDIR /ui
COPY ui/management-ui/package*.json ./
RUN npm install
COPY ui/management-ui .
# DefinitionGraph.vue imports from ../../../definition-editor/js/io.js at build time
COPY ui/definition-editor /definition-editor
RUN npm run build

# ── Stage 2: Python BFF ───────────────────────────────────────────────────────
FROM python:3.13-slim

WORKDIR /app

COPY ui/bff/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ui/bff/main.py .
COPY --from=ui-build /ui/dist ./static
COPY ui/definition-editor ./static/definition-editor

EXPOSE 9000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9000"]
