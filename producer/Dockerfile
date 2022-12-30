FROM node
ARG NODE_ENV
ENV NODE_ENV $NODE_ENV
ENV HOST_IP $HOST_IP

WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install

COPY . .
CMD ["node", "index.js"]
