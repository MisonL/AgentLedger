FROM oven/bun:1.3.9

WORKDIR /app
COPY . .
RUN bun install --frozen-lockfile

WORKDIR /app/apps/web-console
ARG VITE_API_BASE_URL
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}
RUN bun run build

EXPOSE 5173
CMD ["bun", "run", "preview", "--", "--host", "0.0.0.0", "--port", "5173"]
