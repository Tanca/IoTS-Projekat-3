FROM node:20-alpine

WORKDIR /app/analytics-service

COPY analytics-service/package*.json ./
RUN npm install --omit=dev

# The shared event-generator + dataset are copied in so the dashboard-driven
# "live stream" control can replay real buoy readings onto MQTT.
COPY shared/event-generator /app/shared/event-generator
COPY shared/sample-data /app/shared/sample-data
COPY analytics-service/src ./src
COPY analytics-service/public ./public

EXPOSE 8080
CMD ["node", "src/index.js"]
