FROM node:22-alpine

WORKDIR /app/apps/web

COPY apps/web/package.json apps/web/package-lock.json ./
RUN npm ci

COPY apps/web ./

ARG CALCIO_API_BASE_URL=http://api:8000/api/v1
ENV CALCIO_API_BASE_URL=${CALCIO_API_BASE_URL}
ENV NODE_ENV=production
ENV HOSTNAME=0.0.0.0
ENV PORT=3000

RUN npm run build

EXPOSE 3000

CMD ["npm", "run", "start", "--", "--hostname", "0.0.0.0", "--port", "3000"]
