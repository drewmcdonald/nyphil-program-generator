install:
	conda env update -n nyphil-program-generator --file=environment.yaml

install-dev: install
	conda env update -n nyphil-program-generator --file=environment-dev.yaml

fmt:
	isort --recursive .

test:
	isort --recursive --check-only .