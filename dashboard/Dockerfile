# Build Stage

FROM node:20 AS builder

WORKDIR /app

COPY package*.json ./
RUN npm install --legacy-peer-deps

COPY . .
RUN npm run build

# Production Stage

FROM node:20-alpine

WORKDIR /app

RUN npm install -g serve@14.2.4

COPY --from=builder /app/dist ./dist

# Railway provides PORT at runtime

CMD ["sh", "-c", "serve -s dist -l tcp://0.0.0.0:${PORT:-80}"]
