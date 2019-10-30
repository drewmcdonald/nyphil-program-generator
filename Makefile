install:
	conda env update -n nyphil-program-generator --file=environment.yaml

install-dev: install
	conda env update -n nyphil-program-generator --file=environment-dev.yaml

init:
	un-comment-this-line
	dotenv run scripts/0_download.sh
	dotenv run python scripts/1_load_raw_data.py > load.log.txt
	dotenv run python scripts/2_post_load_clean.py
	dotenv run python scripts/3_mbz_composer_scrape.py
	dotenv run python scripts/4_mbz_composer_patch.py

fmt:
	isort --recursive .
	black .

check:
	isort --recursive --check-only .
	black --check .
	flake8 .
	mypy .
