install:
	conda env update -n nyphil-program-generator --file=environment.yaml

install-dev: install
	conda env update -n nyphil-program-generator --file=environment-dev.yaml

fmt:
	isort --recursive .
	black .

check:
	isort --recursive --check-only .
	black --check .