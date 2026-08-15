FROM node:22-alpine

RUN addgroup -S mq && adduser -S mq -G mq
WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY src/ ./src/

USER mq

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://localhost:8080/health 2>/dev/null || \
      nc -z localhost 8080 && echo OK || exit 1

ENV TCP_HOST=broker \
    TCP_PORT=9099   \
    WS_PORT=8080

CMD ["node", "src/index.js"]
