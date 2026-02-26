FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install

COPY tsconfig.json ./
COPY src ./src
COPY migrations ./migrations

EXPOSE 3000

CMD ["npm", "run", "dev"]
