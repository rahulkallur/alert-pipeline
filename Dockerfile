FROM node:lts-alpine

# Create app directory
WORKDIR /usr/src/app

RUN apk add --no-cache curl bash git

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY package*.json ./

RUN npm install
# If you are building your code for production
# RUN npm ci --only=production

# Bundle app source
COPY . .

EXPOSE 80
#CMD [ "node", "index.js" ]
CMD ["node","index.js"]
