FROM node:22-alpine
RUN addgroup -S mq && adduser -S mq -G mq
WORKDIR /app
COPY package.json gatewayClient.js ./
RUN npm install --omit=dev
COPY index.js ./
USER mq
ENV GATEWAY_WS=ws://gateway:8080 PAYMENT_MAX_AMOUNT=200000 PAYMENT_PROCESS_MS=800
CMD ["node", "index.js"]
