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
# Expose port explicitly so Railway's proxy locks onto it
EXPOSE 8080
CMD ["sh", "-c", "serve -s dist -l tcp://0.0.0.0:8080"]
