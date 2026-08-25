FROM node:22-alpine AS build
WORKDIR /app
COPY apps/dashboard/package*.json ./
RUN npm install
COPY apps/dashboard .
RUN npm run build
FROM node:22-alpine
WORKDIR /app
COPY --from=build /app ./
CMD ["npm", "start"]

