FROM node:20-alpine

WORKDIR /app/ingestion-service

COPY ingestion-service/package*.json ./
RUN npm install --omit=dev

# The shared event-generator + dataset are copied in so the require() paths
# (../../shared/...) resolve inside the container.
COPY shared/event-generator /app/shared/event-generator
COPY shared/sample-data /app/shared/sample-data
COPY ingestion-service/src ./src

CMD ["node", "src/index.js"]
