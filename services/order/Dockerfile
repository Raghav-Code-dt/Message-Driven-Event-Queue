FROM node:22-alpine
RUN addgroup -S mq && adduser -S mq -G mq
WORKDIR /app
COPY package.json gatewayClient.js ./
RUN npm install --omit=dev
COPY index.js ./
USER mq
ENV GATEWAY_WS=ws://gateway:8080 ORDER_INTERVAL_MS=3000
CMD ["node", "index.js"]
