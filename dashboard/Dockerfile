# Build Stage
FROM node:20 AS builder
WORKDIR /app
COPY package*.json ./
RUN npm install --legacy-peer-deps
COPY . .
RUN npm run build

# Serve Stage
FROM node:20-alpine
WORKDIR /app
RUN npm install -g serve
COPY package.json ./
COPY --from=builder /app/dist ./dist
# Railway provides the PORT environment variable natively
CMD ["sh", "-c", "serve -s dist -l ${PORT:-80}"]
