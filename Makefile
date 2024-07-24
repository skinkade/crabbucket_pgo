.PHONY: test

test:
	podman-compose down -v && \
	podman-compose up -d && \
	sleep 2 && \
	gleam test && \
	podman-compose down -v