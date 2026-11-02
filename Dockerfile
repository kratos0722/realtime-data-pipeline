FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir ".[kafka]"

# `pipeline` is the console entry point; compose passes the subcommand + args.
ENTRYPOINT ["pipeline"]
CMD ["demo"]
