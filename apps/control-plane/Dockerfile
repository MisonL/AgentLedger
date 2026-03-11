FROM oven/bun:1.3.9

WORKDIR /app
COPY . .
RUN bun install --frozen-lockfile

WORKDIR /app/apps/control-plane
ENV PORT=8080
EXPOSE 8080
CMD ["bun", "run", "start"]
