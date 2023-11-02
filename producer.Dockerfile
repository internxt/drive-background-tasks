FROM node:iron-slim
LABEL author="internxt"

WORKDIR /app

COPY . .

# Install deps
RUN yarn && yarn build && yarn --production && yarn cache clean

# Start server
CMD yarn start:prod:producer