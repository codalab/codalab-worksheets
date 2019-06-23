FROM node:10

ENV NPM_CONFIG_LOGLEVEL warn
ENV PATH /opt/frontend/node_modules/.bin:$PATH

WORKDIR /opt/frontend
RUN npm install react-scripts@1.1.1 -g

COPY frontend/package*.json ./
COPY frontend/graphics ./graphics
COPY frontend/public ./public

RUN npm install

CMD [ "npm", "start" ]
