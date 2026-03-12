# ── Build stage ───────────────────────────────────────────────────────────────
FROM node:20-alpine AS deps
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev && npm cache clean --force
 
# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM node:20-alpine AS runtime
WORKDIR /app
 
# Non-root user for security
RUN addgroup -S monitor && adduser -S monitor -G monitor
 
COPY --from=deps /app/node_modules ./node_modules
COPY server.js package.json ./
 
RUN chown -R monitor:monitor /app
USER monitor
 
ENV NODE_ENV=production
ENV PORT=3000
 
EXPOSE 3000
 
# Tini handles PID 1 signal forwarding (already in node:alpine)
ENTRYPOINT ["node", "server.js"]
 