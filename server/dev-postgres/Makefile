MAKEFLAGS = --no-print-directory --always-make --silent
MAKE = make $(MAKEFLAGS)

# A local build; .github/workflows/beezer-postgres.yml publishes the image.
build-image:
	docker build -t ghcr.io/beezer-pet/postgresql:18.7 .
