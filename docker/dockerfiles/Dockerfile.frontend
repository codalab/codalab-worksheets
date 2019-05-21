FROM node:10

ENV NPM_CONFIG_LOGLEVEL warn

RUN npm install -g serve
CMD [ "serve", "-s", "build", "-l", "2700" ]
EXPOSE 2700

WORKDIR /opt/frontend
COPY frontend/package*.json ./
RUN npm install

COPY frontend .
RUN npm run build --production
